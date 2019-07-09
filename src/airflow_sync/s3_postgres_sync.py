# -*- coding: utf-8 -*-
import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

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
    get_s3_files,
)


log = LoggingMixin().log

dag_default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=0.25),
    "max_active_runs": 1,
    "concurrency": 32,
}


def create_dag(
    dag_name: str,
    pg_conn_id: str = None,
    s3_conn_id: str = None,
    owner: str = None,
    variable_key: str = None,
):
    if variable_key is None:
        variable_key = dag_name
    CONSTANTS = Variable.get(variable_key, deserialize_json=True)
    S3_CONNECTION = CONSTANTS.get("s3_connection")
    PG_CONN_ID = CONSTANTS.get("postgres_connection")
    S3_BUCKET = CONSTANTS.get("s3_bucket")
    SYNC_DELTA = CONSTANTS.get("sync_delta", {"days": -10})
    SYNC_INTERVAL = CONSTANTS.get("sync_interval", "10 5 * * *")
    if owner is not None:
        dag_default_args.update({"owner": owner})
    dag = DAG(
        dag_name,
        default_args=dag_default_args,
        schedule_interval=SYNC_INTERVAL,
        catchup=False,
    )

    files = get_s3_files(S3_CONNECTION, S3_BUCKET)
    log.debug("Found S3 Files: {0}".format(files))

    def get_s3_uri(filename: str, **context) -> str:
        new_uri = f"s3://{S3_BUCKET}/{filename}"
        log.debug(f"converting uri to s3 format: {filename} -> {new_uri}")
        return new_uri

    def get_s3_uris() -> List[str]:
        s3_uris = [get_s3_uri(fn) for fn in files]
        return s3_uris

    def _sync_join(**context):
        join_results = []
        instance = context["task_instance"]
        for fn in files:
            log.debug(f"Found file: {fn}")
            result = instance.xcom_pull(task_ids=f"load_s3_file.{fn}")
            join_results.append((result, fn))
        return join_results

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
    for s3_uri in get_s3_uris():

        # s3_uri = PythonOperator(
        #     task_id=f"get_s3_uri.{fn}",
        #     python_callable=get_s3_uri,
        #     op_kwargs={"filename": fn},
        #     provide_context=True,
        #     dag=dag,
        # )
        # sync_interval.set_downstream(s3_uri)
        _, _, fn = s3_uri.rpartition("/")
        load_s3_file = PandasToPostgresTableOperator(
            task_id=f"load_s3_file.{fn}",
            conn_id=PG_CONN_ID,
            schema="whs",
            table=fn.rsplit(os.extsep, 2)[0],
            sep="\t",
            compression="gzip",
            filepath=s3_uri,
            # filepath=("{{ task_instance.xcom_pull(task_ids=" f"'get_s3_uri.{fn}')" "}}"),  # noqa
            s3_conn_id=S3_CONNECTION,
            include_index=False,
            quoting=csv.QUOTE_NONE,
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
        sync_interval.set_downstream(load_s3_file)
        sync_join.set_upstream(load_s3_file)
    #     s3_uri.set_downstream(load_s3_file)

    # sync_interval >> s3_uri >> load_s3_file >> sync_join
    return dag
