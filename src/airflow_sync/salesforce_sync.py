# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan

import hashlib
import os
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import parse_execution_date
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow_postgres_plugin.hooks import PostgresHook
from airflow_postgres_plugin.operators import FileToPostgresTableOperator
from airflow_salesforce_plugin.operators import SalesforceToFileOperator

from airflow_sync.sync import _cleanup, _run_sql, _sync_interval, _upsert_table
from airflow_sync.utils import SqlFile, get_sql_dir, get_upsert_mapping

log = LoggingMixin().log


# TODO: paralellize where available
# TODO: get this from airflow config
def create_dag(
    dag_name: str,
    pg_conn_id: str,
    sf_conn_id: str,
    catchup: bool = False,
    owner: str = "airflow",
    sql_files: List[Tuple[str, ...]] = None,
    **dag_defaults,
):
    CONSTANTS = Variable.get(dag_name, deserialize_json=True)
    PG_CONN_ID = CONSTANTS["postgres_connection"]
    SF_CONN_ID = CONSTANTS["salesforce_connection"]
    DEPENDS_ON_PAST = CONSTANTS.get("depends_on_past", False)
    ALLOW_UPSERT = CONSTANTS.get("use_upsert_sql", False)
    SYNC_INTERVAL = CONSTANTS.get("sync_interval", "*/10 * * * *")
    SYNC_DELTA = CONSTANTS.get("sync_delta", {"days": -1})
    START_DATE = datetime(2019, 1, 7)
    CATCHUP = False
    if catchup:
        dag_name = f"{dag_name}_catchup"
        START_DATE = parse_execution_date(
            CONSTANTS.get("catchup_start_date", "2009-01-01")
        )._datetime
        CATCHUP = CONSTANTS.get("catchup_enabled", True)
        SYNC_INTERVAL = CONSTANTS.get("catchup_sync_interval", "@weekly")
        SYNC_DELTA = CONSTANTS.get("catchup_sync_delta", {"days": -8})
        if not CATCHUP:
            return

    # dag definitions -------------------------------------------------------------------
    dag_default_args = {
        "owner": owner,
        "depends_on_past": DEPENDS_ON_PAST,
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "pool": "default",
    }
    dag_default_args.update(dag_defaults)
    dag = DAG(
        dag_name,
        schedule_interval=SYNC_INTERVAL,
        catchup=CATCHUP,
        default_args=dag_default_args,
    )

    upsert_map, upsert_sequence, triggers = get_upsert_mapping(
        sql_files, allow_upsert=ALLOW_UPSERT
    )

    # callable definitions --------------------------------------------------------------
    def _sync_join_inserts(**context):
        join_results = []
        instance = context["task_instance"]
        for sql_file in upsert_sequence:
            table = instance.xcom_pull(
                task_ids=f"file_to_postgres_table.{sql_file.path.stem}"
            )
            join_results.append((table, sql_file.path.stem))
        return join_results

    def _sync_join_triggers(**context):
        join_results = []
        instance = context["task_instance"]
        for trigger in triggers:
            result = instance.xcom_pull(task_ids=f"run_sql.{trigger.path.stem}")
            join_results.append((result, trigger.name))
        return join_results

    def _cleanup_temp_table(context):
        instance = context["task_instance"]
        table_name = instance.task_id.split(".")[-1]
        temp_table = context["task_instance"].xcom_pull(
            task_ids=f"file_to_postgres_table.{table_name}"
        )
        templates_dict = context.get("templates_dict", {}).update(
            {"temp_table": temp_table}
        )
        context["templates_dict"] = templates_dict
        _cleanup(PG_CONN_ID, schema="salesforce", context=context)

    # operator definitions -------------------------------------------------------------
    sync_interval = PythonOperator(
        task_id="sync_interval",
        python_callable=_sync_interval,
        op_kwargs={"delta": SYNC_DELTA},
        dag=dag,
        provide_context=True,
        templates_dict={
            "polling_interval.start_date": (
                "{{ execution_date._datetime.replace(tzinfo=None).isoformat() }}"
            )
        },
    )

    sync_join_inserts = PythonOperator(
        task_id="sync_join",
        python_callable=_sync_join_inserts,
        trigger_rule="all_done",
        provide_context=True,
        dag=dag,
    )

    sync_join_triggers = PythonOperator(
        task_id="sync_join_triggers",
        python_callable=_sync_join_triggers,
        trigger_rule="all_done",
        provide_context=True,
        dag=dag,
    )

    tasks: Dict[str, List[Any]] = {}

    tasks = {
        "salesforce_to_file": [],
        "file_to_postgres_table": [],
        "sync_join": [],
        "upsert": [],
        "triggers": [],
        "cleanup": [],
    }
    trigger_dict: Dict[str, SqlFile] = {}

    for i, sql_file in enumerate(upsert_sequence):
        # TODO: Find everywhere else that we use sync interval and need to swap to ranges
        salesforce_to_file = SalesforceToFileOperator(
            task_id=f"salesforce_to_file.{sql_file.path.stem}",
            conn_id=SF_CONN_ID,
            soql=sql_file.sql,
            soql_args=(
                "{{ [task_instance.xcom_pull(task_ids='sync_interval') + 'Z',"
                "execution_date._datetime.replace(tzinfo=None).isoformat() + 'Z']"
                " | join(',') }}"
            ),
            dag=dag,
        )
        tasks["salesforce_to_file"].append(salesforce_to_file)

        file_to_postgres_table = FileToPostgresTableOperator(
            task_id=f"file_to_postgres_table.{sql_file.path.stem}",
            conn_id=PG_CONN_ID,
            table=sql_file.path.stem,
            schema="salesforce",
            filepath=(
                "{{ task_instance.xcom_pull("
                f"task_ids='salesforce_to_file.{sql_file.path.stem}'"
                ") }}"
            ),
            on_failure_callback=_cleanup_temp_table,
            on_retry_callback=_cleanup_temp_table,
            temp_table=True,
            dag=dag,
        )
        tasks["file_to_postgres_table"].append(file_to_postgres_table)

        upsert = PythonOperator(
            task_id=f"upsert.{sql_file.path.stem}",
            python_callable=_upsert_table,
            op_kwargs={
                "table": sql_file.path.stem,
                "pg_conn_id": PG_CONN_ID,
                "schema": "salesforce",
            },
            provide_context=True,
            templates_dict={
                "polling_interval.start_datetime": (
                    "{{ task_instance.xcom_pull(task_ids='sync_interval') }}"
                ),
                "polling_interval.end_datetime": (
                    "{{ execution_date._datetime.replace(tzinfo=None).isoformat() }}"
                ),
                "from_table": (
                    "{{ task_instance.xcom_pull("
                    f"task_ids='file_to_postgres_table.{sql_file.path.stem}'"
                    ") }}"
                ),
            },
            trigger_rule="all_done",
            on_failure_callback=_cleanup_temp_table,
            dag=dag,
        )

        tasks["upsert"].append(upsert)

        cleanup = PythonOperator(
            task_id=f"cleanup.{sql_file.path.stem}",
            python_callable=_cleanup,
            op_kwargs={"pg_conn_id": PG_CONN_ID, "schema": "salesforce"},
            provide_context=True,
            templates_dict={
                "temp_table": (
                    "{{ task_instance.xcom_pull("
                    f"task_ids='file_to_postgres_table.{sql_file.path.stem}'"
                    ") }}"
                )
            },
            dag=dag,
            trigger_rule="all_done",
        )

        tasks["cleanup"].append(cleanup)

        (
            sync_interval
            >> salesforce_to_file
            >> file_to_postgres_table
            >> upsert
            >> cleanup
            >> sync_join_inserts
        )
        for trigger_file in sql_file.triggers:
            trigger = PythonOperator(
                task_id=f"run_sql.{trigger_file.path.stem}",
                python_callable=_run_sql,
                op_kwargs={
                    "pg_conn_id": PG_CONN_ID,
                    "schema": "public",
                    "query": trigger_file.sql,
                    "returns_rows": False,
                },
                provide_context=True,
                templates_dict={
                    "polling_interval.start_datetime": (
                        "{{ task_instance.xcom_pull(task_ids='sync_interval') }}"
                    ),
                    "polling_interval.end_datetime": (
                        "{{ execution_date._datetime.replace(tzinfo=None).isoformat() }}"
                    ),
                },
                trigger_rule="all_done",
                on_failure_callback=_cleanup_temp_table,
                dag=dag,
            )

            has_trigger_deps = any(dep.is_trigger for dep in trigger_file.dependencies)
            if not has_trigger_deps:
                sync_join_inserts >> trigger
            # * Loop through the subdependencies of the current action for anything that
            # * depends on a trigger -- these need to be added downstream
            # * e.g. we are currently adding "account", one subdependency is
            # * "upsert account" which may have additional triggers which are fired by
            # * performing that upsert and which must be properly set downstream of it
            for subdep in filter(lambda x: x.is_trigger, trigger_file.dependencies):
                # * We can skip the non-trigger deps because they are resolved
                # * during the sync_join above
                trigger_dict[subdep.name] >> trigger
            if not trigger_file.dependants:
                trigger >> sync_join_triggers

            trigger_dict[trigger_file.name] = trigger
            tasks["triggers"].append(trigger)
    return dag
