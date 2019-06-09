# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan

from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow_salesforce_plugin.hooks import SalesforceHook
from airflow_salesforce_plugin.operators import SalesforceAttachmentToS3Operator

from airflow_sync.sync import _cleanup, _sync_interval, _upsert_table, query_salesforce
from airflow_sync.utils import get_sql_dir

# dag definitions ----------------------------------------------------------------------


def create_dag(
    dag_name: str,
    sf_conn_id: str,
    s3_conn_id: str,
    dag_base: str = None,
    owner: str = "airflow",
):
    if dag_base is not None:
        CONSTANTS = Variable.get(dag_base, deserialize_json=True)
    else:
        CONSTANTS = Variable.get(dag_name, deserialize_json=True)
    ATTACHMENT_SQL_PATH = get_sql_dir() / "salesforce" / "attachment.sql"
    S3_CONN_ID = CONSTANTS["s3_connection"]
    SF_CONN_ID = CONSTANTS["salesforce_connection"]
    S3_BUCKET = CONSTANTS["s3_bucket"]
    DEPENDS_ON_PAST = CONSTANTS.get("depends_on_past", False)
    SYNC_INTERVAL = CONSTANTS.get("attachment_sync_interval", "*/20 * * * *")
    SYNC_DELTA = CONSTANTS.get("sync_delta", {"days": -1})
    START_DATE = datetime(2019, 1, 28)
    UPLOAD_TIMEOUT = int(CONSTANTS["upload_timeout"])
    CONCURRENT_UPLOADS = int(CONSTANTS["concurrent_uploads"])
    CATCHUP = False

    dag = DAG(
        dag_name,
        default_args={
            "owner": "cx",
            "depends_on_past": DEPENDS_ON_PAST,
            "start_date": START_DATE,
            "retries": 1,
            "retry_delay": timedelta(seconds=10),
        },
        schedule_interval=SYNC_INTERVAL,
        catchup=CATCHUP,
        owner=owner,
    )

    # operator definitions --------------------------------------------------------------

    sync_interval = PythonOperator(
        task_id="sync_interval",
        python_callable=_sync_interval,
        op_kwargs={"delta": SYNC_DELTA},
        dag=dag,
        provide_context=True,
        templates_dict={"polling_interval.start_date": "{{ ts }}"},
    )

    # FIXME: This is a redundant execution, we should feed this through the other
    # pipeline as a branch that runs every other time, pulling ids from salesforce
    # and kicking off the attachment sync job
    attachment_ids = PythonOperator(
        task_id="attachment_ids",
        python_callable=query_salesforce,
        provide_context=True,
        op_kwargs={"sf_conn_id": SF_CONN_ID, "soql": ATTACHMENT_SQL_PATH.read_text()},
        templates_dict={
            "soql_args": (
                "{{ task_instance.xcom_pull(task_ids='sync_interval') }}Z,{{ ts }}Z"
            )
        },
        dag=dag,
    )

    get_attachments = SalesforceAttachmentToS3Operator(
        task_id="get_attachments",
        concurrent_uploads=CONCURRENT_UPLOADS,
        attachment_ids="{{ task_instance.xcom_pull(task_ids='attachment_ids') }}",
        conn_id=SF_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        s3_bucket=S3_BUCKET,
        upload_timeout=UPLOAD_TIMEOUT,
        api_version="v38.0",
        provide_context=True,
        dag=dag,
    )

    sync_interval >> attachment_ids >> get_attachments
    return dag
