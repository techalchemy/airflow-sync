# airflow-sync
Sync Toolkit for Apache Airflow


## Usage

To use this module, install the repository and use it to build dags with:

```console
    $ pip install -e git+git@github.com:snapmetrix/airflow-sync.git#egg=airflow-sync
```

Then you can simply build a dag:

```python
from airflow_sync.csv_sync_s3 import create_dag

DAG_NAME = "csv_s3_sync_dag"
CSV_DIRPATH = Path("/data_files")
PG_CONN_ID = Variable.get("csv_s3_postgres_conn_id")
S3_CONN_ID = Variable.get("csv_s3_conn_id")
S3_BUCKET = Variable.get("csv_s3_sync.s3_bucket")
CONCURRENT_UPLOADS = int(Variable.get("csv_s3_sync.concurrent_uploads"))
UPLOAD_TIMEOUT = int(Variable.get("csv_s3_sync.upload_timeout"))
SYNC_INTERVAL = Variable.get("csv_s3_sync.sync_interval")


# dag definitions ----------------------------------------------------------------------
dag = create_dag(
    DAG_NAME,
    PG_CONN_ID,
    S3_CONN_ID,
    S3_BUCKET,
    SYNC_INTERVAL,
    concurrent_uploads=CONCURRENT_UPLOADS,
    upload_timeout=UPLOAD_TIMEOUT,
    csv_dir=CSV_DIRPATH,
    owner="dag_owner"
)
```
