# -*- coding: utf-8 -*-
import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, NamedTuple, Optional

import airflow
from airflow import DAG
from airflow.hooks.postgres_plugin import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow_postgres_plugin.operators import (
    PandasToPostgresBulkOperator,
    PandasToPostgresTableOperator,
)

from airflow_sync.sync import (  # noqa  # isort:skip
    _cleanup,
    _run_sql,
    _sync_interval,
    _upsert_table,
    _cleanup,
    get_s3_files,
)


class S3File(NamedTuple):
    uri: str = ""
    filename: str = ""
    table: str = ""


log = LoggingMixin().log

dag_default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=0.25),
    "max_active_runs": 1,
    "concurrency": 16,
}


def create_dag(
    dag_name: str,
    pg_conn_id: str = None,
    s3_conn_id: str = None,
    owner: str = None,
    variable_key: str = None,
    tables: List[str] = None,
    pool: str = None,
    **dag_defaults,
):
    if variable_key is None:
        variable_key = dag_name
    CONSTANTS = Variable.get(variable_key, deserialize_json=True)
    S3_CONNECTION = CONSTANTS.get("s3_connection")
    PG_CONN_ID = CONSTANTS.get("postgres_connection")
    S3_BUCKET = CONSTANTS.get("s3_bucket")
    SYNC_DELTA = CONSTANTS.get("sync_delta", {"days": -10})
    SYNC_INTERVAL = CONSTANTS.get("sync_interval", "10 5 * * *")
    default_owner = owner if owner is not None else dag_default_args["owner"]
    default_pool = pool if pool is not None else dag_default_args["pool"]
    dag_default_args.update({"owner": default_owner, "pool": default_pool})
    dag_default_args.update(dag_defaults)
    dag = DAG(
        dag_name,
        default_args=dag_default_args,
        schedule_interval=SYNC_INTERVAL,
        catchup=False,
    )

    if not tables:
        tables = [fn.split(".")[0] for fn in get_s3_files(S3_CONNECTION, S3_BUCKET)]

    def get_s3_uri(filename: str, **context) -> str:
        new_uri = f"s3://{S3_BUCKET}/{filename}"
        log.debug(f"converting uri to s3 format: {filename} -> {new_uri}")
        return new_uri

    def convert_table_to_file(table: str) -> str:
        if "://" in table:
            table = Path(table).stem
        formatted_date = (datetime.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
        return f"{table}.csv_{formatted_date}.gz"

    def get_s3_uris(tables: Optional[List[str]]) -> List[str]:
        if not tables:
            tables = [
                fn.split(".")[0] for fn in get_s3_files(S3_CONNECTION, S3_BUCKET)
            ]  # type: ignore  # noqa:E501
        s3_uris = [get_s3_uri(fn) for fn in tables]
        return s3_uris

    files = [(table, convert_table_to_file(table)) for table in tables]
    uris = [S3File(uri=get_s3_uri(fn), filename=fn, table=table) for table, fn in files]

    def _sync_join(**context):
        join_results = []
        instance = context["task_instance"]
        for fn in uris:
            result = instance.xcom_pull(task_ids=f"load_s3_file.{fn.table}")
            join_results.append((result, fn))
        return join_results

    def _cleanup_temp_table(context):
        instance = context["task_instance"]
        table_name = instance.task_id.split(".")[-1]
        temp_table = context["task_instance"].xcom_pull(
            task_ids=f"load_s3_file.{table_name}"
        )
        templates_dict = context.get("templates_dict", {}).update(
            {"temp_table": temp_table}
        )
        context["templates_dict"] = templates_dict
        _cleanup(PG_CONN_ID, schema="whs", context=context)

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

    # load_s3_files = PandasToPostgresBulkOperator(
    #     task_id="load_s3_files",
    #     conn_id=PG_CONN_ID,
    #     schema="whs",
    #     sep="\t",
    #     compression="gzip",
    #     filepaths="{{ task_instance.xcom_pull(task_ids='get_s3_uris') }}",
    #     s3_conn_id=S3_CONNECTION,
    #     include_index=False,
    #     provide_context=True,
    #     quoting=csv.QUOTE_NONE,
    #     templates_dict={
    #         "polling_interval.start_datetime": (
    #             "{{ task_instance.xcom_pull(task_ids='sync_interval') }}"
    #         ),
    #         "polling_interval.end_datetime": (
    #             "{{ execution_date._datetime.replace(tzinfo=None).isoformat() }}"
    #         ),
    #     },
    #     dag=dag,
    # )

    sync_join = PythonOperator(
        task_id="sync_join",
        python_callable=_sync_join,
        trigger_rule="all_done",
        provide_context=True,
        dag=dag,
    )

    # for fn in files:
    for uri in uris:

        # s3_uri = PythonOperator(
        #     task_id=f"get_s3_uri.{fn}",
        #     python_callable=get_s3_uri,
        #     op_kwargs={"filename": fn},
        #     provide_context=True,
        #     dag=dag,
        # )
        # sync_interval.set_downstream(s3_uri)
        # _, _, fn = uri.rpartition("/")
        load_s3_file = PandasToPostgresTableOperator(
            task_id=f"load_s3_file.{uri.table}",
            conn_id=PG_CONN_ID,
            schema="whs",
            table=uri.table,
            sep="\t",
            compression="gzip",
            filepath=uri.uri,
            pool=pool if pool else "default",
            s3_conn_id=S3_CONNECTION,
            include_index=False,
            quoting=csv.QUOTE_NONE,
            temp_table=True,
            on_failure_callback=_cleanup_temp_table,
            on_retry_callback=_cleanup_temp_table,
            templates_dict={
                "polling_interval.start_datetime": (
                    "{{ task_instance.xcom_pull(task_ids='sync_interval') }}"
                ),
                "polling_interval.end_datetime": (
                    "{{ execution_date._datetime.replace(tzinfo=None).isoformat() }}"
                ),
            },
            dag=dag,
        )

        upsert_table = PythonOperator(
            task_id=f"upsert_postgres_table.{uri.table}",
            python_callable=_upsert_table,
            op_kwargs={"table": uri.table, "pg_conn_id": PG_CONN_ID, "schema": "whs"},
            provide_context=True,
            pool=pool if pool else "default",
            templates_dict={
                "polling_interval.start_datetime": (
                    "{{ task_instance.xcom_pull(task_ids='sync_interval') }}"
                ),
                "polling_interval.end_datetime": (
                    "{{ execution_date._datetime.replace(tzinfo=None).isoformat() }}"
                ),
                # fmt: off
                "from_table": (
                    "{{ task_instance.xcom_pull("
                    f"task_ids='load_s3_file.{uri.table}'"
                    ") }}"
                ),
                # fmt: on
            },
            on_failure_callback=_cleanup_temp_table,
            dag=dag,
        )

        cleanup = PythonOperator(
            task_id=f"cleanup.{uri.table}",
            python_callable=_cleanup,
            op_kwargs={"pg_conn_id": PG_CONN_ID, "schema": "whs"},
            provide_context=True,
            pool=pool if pool else "default",
            templates_dict={
                "temp_table": (
                    "{{ task_instance.xcom_pull("
                    f"task_ids='load_s3_file.{uri.table}'"
                    ") }}"
                )
            },
            dag=dag,
            trigger_rule="all_done",
        )

        sync_interval.set_downstream(load_s3_file)
        load_s3_file.set_downstream(upsert_table)
        upsert_table.set_downstream(cleanup)
        sync_join.set_upstream(cleanup)
    #     s3_uri.set_downstream(load_s3_file)

    # sync_interval >> s3_uri >> load_s3_file >> sync_join
    return dag
