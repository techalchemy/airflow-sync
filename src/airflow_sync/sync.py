# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan

import concurrent
import concurrent.futures
import gzip
import hashlib
import json
import os
import shutil
import tempfile
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import sqlalchemy
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow_postgres_plugin.hooks import PostgresHook
from airflow_postgres_plugin.operators import FileToPostgresTableOperator
from airflow_salesforce_plugin.hooks import SalesforceHook
from airflow_salesforce_plugin.operators import SalesforceToFileOperator
from sqlalchemy.dialects.postgresql import insert

log = LoggingMixin().log


def _sync_interval(delta: Optional[Dict[str, int]], **context) -> str:
    if not isinstance(delta, dict):
        delta = {"days": -10}
    templates_dict: Dict[str, str] = context.get("templates_dict", {})
    if "polling_interval.start_date" in templates_dict:
        start_datetime = datetime.fromisoformat(
            templates_dict["polling_interval.start_date"]
        ).replace(tzinfo=None)
    else:
        start_datetime = datetime.now()
    return (start_datetime + timedelta(**delta)).isoformat()


def _run_sql(
    pg_conn_id: str,
    query: str,
    schema: Optional[str] = "public",
    returns_rows: bool = True,
    **context,
) -> List[Tuple[Any, ...]]:
    templates_dict: Dict[str, str] = context.get("templates_dict", {})
    start_datetime: str = templates_dict["polling_interval.start_datetime"]
    end_datetime: str = templates_dict["polling_interval.end_datetime"]
    pg_hook: PostgresHook = PostgresHook(pg_conn_id, schema=schema)
    results = list(
        pg_hook.query(query, [start_datetime, end_datetime], returns_rows=returns_rows)
    )
    return results


def _upsert_table(
    pg_conn_id: str,
    table: str,
    schema: Optional[str],
    constraints: List[str] = None,
    returns_rows: bool = True,
    **context,
) -> None:
    if not constraints:
        constraints = []

    templates_dict: Dict[str, str] = context.get("templates_dict", {})
    start_datetime: str = templates_dict["polling_interval.start_datetime"]
    end_datetime: str = templates_dict["polling_interval.end_datetime"]
    table_name: str = templates_dict["from_table"]
    pg_hook: PostgresHook = PostgresHook(pg_conn_id, schema=schema)
    from_table: sqlalchemy.Table = pg_hook.get_sqlalchemy_table(table_name)
    to_table: sqlalchemy.Table = pg_hook.get_sqlalchemy_table(table)

    insert_statement: sqlalchemy.sql.dml.Insert = insert(to_table).from_select(
        from_table.columns.keys(), from_table.select()
    )

    inspected: sqlalchemy.Table = sqlalchemy.inspect(to_table)
    primary_keys: List[str] = [_.name for _ in inspected.primary_key]
    if isinstance(constraints, list) and len(constraints) > 0:
        primary_keys = constraints

    upsert_statement: sqlalchemy.sql.dml.Insert = insert_statement.on_conflict_do_update(
        index_elements=primary_keys,
        set_={
            column.name: getattr(insert_statement.excluded, column.name)
            for column in inspected.columns
        },
    )

    with pg_hook.sqlalchemy_session() as session:
        session.execute(
            sqlalchemy.text(
                "SET polling_interval.start_datetime TO :start_datetime;"
                "SET polling_interval.end_datetime TO :end_datetime;"
            ),
            dict(start_datetime=start_datetime, end_datetime=end_datetime),
        )
        session.execute(sqlalchemy.text(str(upsert_statement)))
    return None


def _cleanup(pg_conn_id: str, schema: str = "public", **context) -> None:
    pg_hook: PostgresHook = PostgresHook(pg_conn_id, schema=schema)
    templates_dict: Dict[str, str] = context.get("templates_dict", {})
    if "temp_table" not in templates_dict:
        return None
    temp_table_name = templates_dict["temp_table"]
    temp_table: sqlalchemy.Table = pg_hook.get_sqlalchemy_table(temp_table_name)
    temp_table.drop(pg_hook.get_sqlalchemy_engine())
    return None


def query_salesforce(sf_conn_id: str, soql: str, **context) -> Union[List[str], str]:
    sf_hook = SalesforceHook(sf_conn_id)
    templates_dict: Dict[str, str] = context.get("templates_dict", {})
    if "soql_args" not in templates_dict:
        raise ValueError("Must provide *soql_args* as a parameter to query salesforce!")
    soql_args = templates_dict["soql_args"]
    query = sf_hook.query(soql, soql_args.split(","), include_headers=True)
    column_index = next(query).index("id")
    log.info(f"acquiring attachment list using {query!r} on {sf_hook!r}")
    id_list = ",".join([_[column_index] for _ in query if _[column_index] is not None])
    return id_list


def upload_file_to_s3(
    s3_bucket: str,
    path: Optional[str] = None,
    s3_conn_id: str = None,
    s3_key: Optional[str] = None,
    s3_hook: S3Hook = None,
    **context,
) -> str:
    templates_dict: Dict[str, str] = context.get("templates_dict", {})
    path = templates_dict.get("path", path)
    if path is None:
        raise TypeError(f"Expected a value for path, received {path!r}")
    filepath = Path(path)
    if s3_key is None:
        s3_key = f"{filepath.stem}"
    record_key = f"{s3_key}_{date.today().isoformat()}.gz"
    if s3_hook is None:
        if s3_conn_id is None:
            raise TypeError(f"Expected string for s3_conn_id, received {s3_conn_id!r}")
        s3_hook = S3Hook(s3_conn_id)
    if s3_hook.check_for_key(record_key, bucket_name=s3_bucket):
        log.warning(f"overwriting existing file for {record_key!r} in {s3_hook!r}")
    else:
        log.info(f"creating object from {filepath!r} at {record_key!r} in {s3_hook!r}")

    log.info(f"Loading file contents into s3 at {record_key} in bucket {s3_bucket}")
    try:
        s3_hook.load_bytes(filepath.read_bytes(), record_key, bucket_name=s3_bucket)
    except Exception as exc:
        log.error(
            f"error occured while trying to upload attachment from {filepath!r}"
            f" to key {record_key!r} in {s3_hook!r}, {exc}"
        )
        raise exc
    log.info(f"Successfully uploaded file to key: {record_key}")
    return record_key


def gzip_file(filepath: str) -> str:
    f_out = None
    path = Path(filepath)
    log.info(f"Reading file: {path.as_posix()}")
    temp_path = Path(tempfile.gettempdir()) / f"{path.name}.gz"
    temp_filename = temp_path.as_posix()
    with open(path.as_posix(), "rb") as f_in:
        with gzip.open(temp_filename, "wb") as f_out:
            log.info(f"Writing gzipped file to tempfile: {temp_filename}")
            shutil.copyfileobj(f_in, f_out)
    return temp_filename


def gzip_files(filepaths: List[str], max_workers: int = 10, **context) -> str:
    results: List[str] = []
    result_map: Union[Iterator, List] = []

    if filepaths:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            result_map = executor.map(gzip_file, filepaths)
    results = [r for r in result_map if r is not None]
    return ",".join(results)


def _upload_file(path_and_hook: Tuple[str, S3Hook], bucket: str):
    path, s3_hook = path_and_hook
    filepath = Path(path)
    s3_key = f"{filepath.stem}"
    record_key = f"{s3_key}_{date.today().isoformat()}.gz"
    if s3_hook.check_for_key(record_key, bucket_name=bucket):
        log.warning(f"overwriting existing file for {record_key!r} in {s3_hook!r}")
    else:
        log.info(f"creating object from {filepath!r} at {record_key!r} in {s3_hook!r}")

    log.info(f"Loading file contents into s3 at {record_key} in bucket {bucket}")
    try:
        s3_hook.load_bytes(filepath.read_bytes(), record_key, bucket_name=bucket)
    except Exception as exc:
        log.error(
            f"error occured while trying to upload attachment from {filepath!r}"
            f" to key {record_key!r} in {s3_hook!r}, {exc}"
        )
        raise exc
    log.info(f"Successfully uploaded file to key: {record_key}")
    return record_key


def upload_files_to_s3(
    s3_conn_id: str, s3_bucket: str, max_connections: int = 10, **context
) -> str:
    results: List[str] = []
    result_map: Union[Iterator, List] = []
    templates_dict: Dict[str, str] = context.get("templates_dict", {})
    filepaths: str = templates_dict.get("filepaths", "").strip()

    def upload_file(filepath_and_hook: Tuple[str, S3Hook], bucket: str = s3_bucket):
        return _upload_file(filepath_and_hook, bucket)

    if filepaths:
        log.info(f"Connecting to s3 connection: {s3_conn_id}")
        hook = S3Hook(s3_conn_id)
        filepath_list = filepaths.split(",")
        paths = [(os.path.abspath(fp), hook) for fp in filepath_list]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_connections
        ) as executor:
            result_map = executor.map(upload_file, paths)
    results = [r for r in result_map if r is not None]
    return ",".join(results)
