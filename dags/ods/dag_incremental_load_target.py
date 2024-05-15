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
dag_id = "dag_incremental_load_target"
snowflake_conn_id = "snow_devtest"
snowflake_sp = "ODS.META_DATA.INCREMENTAL_LOAD_TARGET_ASTRO"

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

    call_sp_sql = f"call {snowflake_sp}(%(initial_flag)s, %(source_db)s, %(source_schema)s, %(source_stream)s, %(target_db)s, %(target_schema)s, %(target_table)s, %(mapping_id)s)"
    params = {
        'initial_flag':"N",
        'source_db':"FIVETRAN",
        'source_schema':"MCONNECT",
        'source_stream':"ACCOUNT_STREAM",
        'target_db':"ODS",
        'target_schema':"MCONNECT",
        'target_table':"ACCOUNT", 
        'mapping_id':"ID"
    }

    incremental_load_target = SnowflakeOperator(
        task_id = "incremental_load_target",
        sql = call_sp_sql,
        autocommit = True,
        parameters = params,
    )

    end = EmptyOperator(task_id="end")

    (
        begin >> incremental_load_target >> end
    )
