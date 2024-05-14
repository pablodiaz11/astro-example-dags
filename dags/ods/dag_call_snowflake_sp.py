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
dag_id = "dag_call_snowflake_sp"
snowflake_conn_id = "snow_devtest"
snowflake_sp = "STAGE.SP_PROCESS_RUN_END"

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
    tags = ["snowflake_sp"],
    schedule = None,
    catchup = False,
) as dag:
    # Star process
    begin = EmptyOperator(task_id="begin")

    params = {
        'feed_date':"2023-09-27",
        'process_name':"S&P API",
        'status':"Failed"
    }

    #sql_call_sp = f"call {snowflake_sp}(?, ?, ?)"
    sql_call_sp = f"call {snowflake_sp}(%(feed_date)s, %(process_name)s, %(status)s)"

    populate_interaction_company = SnowflakeOperator(
        task_id = "populate_interaction_company",
        sql = sql_call_sp,
        autocommit = True,
        parameters = params,
        #parameters = [params['feed_date'], params['process_name'],params['status']],
    )

    incremental_sp_run = SnowflakeOperator(
        task_id = "incremental_sp_run",
        sql = "call stage.usp_poc2('Hi')",
    )

    incremental_load_target = SnowflakeOperator(
        task_id = "incremental_load_target",
        sql = "call stage.usp_poc4(4,0)",
    )

    end = EmptyOperator(task_id="end")

    (
        begin >> populate_interaction_company >> incremental_sp_run >> incremental_load_target >> end
    )
