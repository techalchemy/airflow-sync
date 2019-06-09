# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan

import os
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import parse_execution_date
from airflow_postgres_plugin.operators import FileToPostgresTableOperator
from airflow_salesforce_plugin.operators import SalesforceToFileOperator

from airflow_sync.sync import _cleanup, _run_sql, _sync_interval, _upsert_table
from airflow_sync.utils import SqlFile, annotated_last


def create_dag(
    dag_name: str,
    pg_conn_id: str,
    sf_conn_id: str,
    catchup: bool = False,
    dag_base: str = None,
    owner: str = "airflow",
    sync_file: SqlFile = None,
    triggers: List[SqlFile] = None,
    deps: List[SqlFile] = None,
):
    if dag_base is not None:
        CONSTANTS = Variable.get(dag_base, deserialize_json=True)
    else:
        CONSTANTS = Variable.get(dag_name, deserialize_json=True)
    PG_CONN_ID = CONSTANTS["postgres_connection"]
    SF_CONN_ID = CONSTANTS["salesforce_connection"]
    DEPENDS_ON_PAST = CONSTANTS.get("depends_on_past", False)
    ALLOW_UPSERT = CONSTANTS.get("use_upsert_sql", False)
    SYNC_INTERVAL = CONSTANTS.get("note_sync_interval", "*/5 * * * *")
    SYNC_DELTA = CONSTANTS.get("sync_delta", {"days": -1})
    START_DATE = datetime(2019, 1, 18)
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

    # dag definitions --------------------------------------------------------------------
    dag = DAG(
        dag_name,
        default_args={
            "owner": owner,
            "depends_on_past": DEPENDS_ON_PAST,
            "start_date": START_DATE,
            "retries": 1,
            "retry_delay": timedelta(minutes=0.5),
        },
        schedule_interval=SYNC_INTERVAL,
        catchup=CATCHUP,
    )

    # callable definitions ---------------------------------------------------------------
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

    def _sync_join_insert(context):
        join_results = []
        instance = context["task_instance"]
        task_name, _, filename = instance.task_id.rpartition(".")
        trigger_file = next(iter(t for t in triggers if t.path.stem == filename), None)
        for dep in trigger_file.dependencies:
            if dep.is_trigger:
                result = instance.xcom_pull(task_ids=f"run_sql{dep.path.stem}")
            else:
                result = context["task_instance"].xcom_pull(
                    task_ids=f"upsert.{dep.path.stem}"
                )
            join_results.append((result, dep.path.stem))
        return join_results

    # DAG Operator Definitions ---------------------------------------------------------
    sync_interval = PythonOperator(
        task_id="sync_interval",
        python_callable=_sync_interval,
        op_kwargs={"delta": SYNC_DELTA},
        provide_context=True,
        dag=dag,
        templates_dict={"polling_interval.start_date": "{{ ts }}"},
    )

    salesforce_to_file = SalesforceToFileOperator(
        task_id=f"salesforce_to_file.{sync_file.path.stem}",
        conn_id=SF_CONN_ID,
        soql=sync_file.sql,
        soql_args="{{ task_instance.xcom_pull(task_ids='sync_interval') }}Z,{{ ts }}Z",
        dag=dag,
    )

    file_to_postgres_table = FileToPostgresTableOperator(
        task_id=f"file_to_postgres_table.{sync_file.path.stem}",
        conn_id=PG_CONN_ID,
        table=sync_file.path.stem,
        schema="salesforce",
        filepath=(
            "{{ task_instance.xcom_pull("
            f"task_ids='salesforce_to_file.{sync_file.path.stem}'"
            ") }}"
        ),
        temp_table=True,
        on_failure_callback=_cleanup_temp_table,
        on_retry_callback=_cleanup_temp_table,
        dag=dag,
    )

    upsert = PythonOperator(
        task_id=f"upsert.{sync_file.path.stem}",
        python_callable=_upsert_table,
        op_kwargs={
            "table": sync_file.path.stem,
            "pg_conn_id": PG_CONN_ID,
            "schema": "salesforce",
        },
        provide_context=True,
        templates_dict={
            "polling_interval": (
                "{{ task_instance.xcom_pull(task_ids='sync_interval') }}"
            ),
            "from_table": (
                "{{ task_instance.xcom_pull("
                f"task_ids='file_to_postgres_table.{sync_file.path.stem}'"
                ") }}"
            ),
        },
        trigger_rule="all_done",
        on_failure_callback=_cleanup_temp_table,
        dag=dag,
    )

    cleanup = PythonOperator(
        task_id="cleanup_salesforce_note",
        python_callable=_cleanup,
        op_kwargs={"pg_conn_id": PG_CONN_ID, "schema": "salesforce"},
        provide_context=True,
        templates_dict={
            "temp_table": (
                "{{ task_instance.xcom_pull("
                f"task_ids='file_to_postgres_table.{sync_file.path.stem}'"
                ") }}"
            )
        },
        dag=dag,
        on_failure_callback=_cleanup_temp_table,
        trigger_rule="all_done",
    )

    sync_interval >> salesforce_to_file >> file_to_postgres_table >> upsert

    if ALLOW_UPSERT:
        for trigger_file, is_last in annotated_last(sync_file.triggers):

            trigger = PythonOperator(
                task_id=f"run_sql.{trigger_file.path.stem}",
                python_callable=_run_sql,
                op_kwargs={
                    "pg_conn_id": PG_CONN_ID,
                    "schema": "public",
                    "query": trigger_file.sql,
                },
                provide_context=True,
                templates_dict={
                    "polling_interval": (
                        "{{ task_instance.xcom_pull(task_ids='sync_interval') }}"
                    )
                },
                trigger_rule="all_done",
                on_failure_callback=_cleanup_temp_table,
                dag=dag,
            )
            if is_last:
                upsert >> trigger >> cleanup
            else:
                upsert >> trigger
        else:
            upsert >> cleanup
    else:
        upsert >> cleanup

    return dag
