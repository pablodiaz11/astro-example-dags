from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

local_tz = pendulum.timezone("America/New_York")
dag_id = "dag_incremental_sp_run"
snowflake_conn_id = "snow_devtest"
snowflake_sp = "ODS.META_DATA.INCREMNETAL_RUN_SP_ASTRO"

default_args={
    'email': ['pablo.diaz@moelis.com'],
    'email_on_failure': True,
    "snowflake_conn_id": snowflake_conn_id,
    "retries": 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id,
    start_date = datetime(2024, 1, 1, tzinfo=local_tz),
    default_args = default_args,
    tags = ["ODS"],
    schedule = None,
    catchup = False,
) as dag:
    # Star process
    begin = EmptyOperator(task_id="begin")

    call_sp_sql = f"call {snowflake_sp}()"

    incremental_sp_run = SnowflakeOperator(
        task_id = "incremental_sp_run",
        sql = call_sp_sql,
        autocommit = True,
    )

    end = EmptyOperator(task_id="end")

    (
        begin >> incremental_sp_run >> end
    )
