# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow_postgres_plugin.hooks import PostgresHook
from airflow_postgres_plugin.operators import PandasToPostgresTableOperator

from airflow_sync.sync import (
    _cleanup,
    _run_sql,
    _sync_interval,
    _upsert_table,
    get_s3_files,
)

dag_default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=0.5),
    "max_active_runs": 1,
    "concurrency": 10,
}


def create_dag(
    dag_name: str,
    pg_conn_id: str = None,
    s3_conn_id: str = None,
    owner: str = "airflow",
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
    dag = DAG(
        dag_name,
        default_args=dag_default_args,
        schedule_interval=SYNC_INTERVAL,
        catchup=False,
        owner=owner,
    )

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

    files = get_s3_files(S3_CONNECTION, S3_BUCKET)

    def get_s3_uri(filename):
        return f"s3://{S3_BUCKET}/{fn}"

    def _sync_join(**context):
        join_results = []
        instance = context["task_instance"]
        for fn in files:
            result = instance.xcom_pull(task_ids=f"load_s3_file.{fn}")
            join_results.append((result, fn))
        return join_results

    sync_join = PythonOperator(
        task_id="sync_join",
        python_callable=_sync_join,
        trigger_rule="all_done",
        provide_context=True,
        dag=dag,
    )

    for fn in files:

        s3_uri = PythonOperator(
            task_id=f"get_s3_uri.{fn}",
            python_callable=get_s3_uri,
            op_kwargs={"filename": fn},
            provide_context=False,
            dag=dag,
        )

        load_s3_file = PandasToPostgresTableOperator(
            task_id=f"load_s3_file.{fn}",
            conn_id=PG_CONN_ID,
            schema="whs",
            table=fn.rsplit(".", len(Path(fn).suffixes))[0],
            sep=r"\t",
            compression="gzip",
            filepath=f"{{ task_instance.xcom_pull(task_ids='get_s3_uri.{fn}') }}",
            s3_conn_id=S3_CONNECTION,
            provide_context=True,
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

        sync_interval >> s3_uri >> load_s3_file >> sync_join
    return dag
