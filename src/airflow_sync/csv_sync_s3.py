# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow_sync.sync import gzip_file, gzip_files, upload_file_to_s3, upload_files_to_s3

# dag definitions ----------------------------------------------------------------------
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
    s3_bucket: str = None,
    sync_interval: str = None,
    dag_base: str = None,
    concurrent_uploads: str = "10",
    upload_timeout: str = "600",
    csv_dir: str = "/data_files",
    owner: str = "airflow",
):
    if pg_conn_id is None or s3_conn_id is None or s3_bucket is None:
        if dag_base is not None:
            CONSTANTS = Variable.get(dag_base, deserialize_json=True)
        else:
            CONSTANTS = Variable.get(dag_name, deserialize_json=True)
        pg_conn_key = next(
            iter([k for k in ("postgres_connection", "pg_connection") if k in CONSTANTS]),
            None,
        )
        s3_conn_id = CONSTANTS["s3_connection"] if not s3_conn_id else s3_conn_id
        pg_conn_id = CONSTANTS[pg_conn_key] if not pg_conn_id else pg_conn_id
        s3_bucket = CONSTANTS["s3_bucket"] if not s3_bucket else s3_bucket
        sync_interval = (
            CONSTANTS.get("sync_interval", "5 6 * * * ")
            if not sync_interval
            else sync_interval
        )
        concurrent_uploads = CONSTANTS.get("concurrent_uploads", concurrent_uploads)
        upload_timeout = CONSTANTS.get("upload_timeout", upload_timeout)
        csv_dir = CONSTANTS.get("csv_dir", csv_dir)
    csv_dirpath = Path(csv_dir)

    upload_timeout_int = int(upload_timeout)  # noqa
    concurrent_uploads_int = int(concurrent_uploads)

    dag = DAG(
        dag_name,
        default_args=dag_default_args,
        schedule_interval=sync_interval,
        catchup=False,
        owner=owner,
    )

    # operator definitions ---------------------------------------------------

    start_task = DummyOperator(task_id="csv_sync_start", dag=dag)

    gzip_task = PythonOperator(
        task_id="gzip_files",
        python_callable=gzip_files,
        provide_context=True,
        op_kwargs={"filepaths": [fp.as_posix() for fp in csv_dirpath.glob("*.csv")]},
        dag=dag,
    )

    upload_files = PythonOperator(
        task_id="upload_files_to_s3",
        provide_context=True,
        python_callable=upload_files_to_s3,
        op_kwargs={
            "s3_conn_id": s3_conn_id,
            "s3_bucket": s3_bucket,
            "max_connections": concurrent_uploads_int,
            "upload_timeout": upload_timeout_int,
        },
        templates_dict={
            "filepaths": (" {{ task_instance.xcom_pull(task_ids='gzip_files') }}")
        },
        dag=dag,
    )
    start_task >> gzip_task >> upload_files
    return dag
