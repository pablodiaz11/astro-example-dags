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
DAG_ID = "dag_populate_interaction_company"
SNOWFLAKE_CONN_ID = "snow_devtest"
SNOWFLAKE_SP = "ODS.META_DATA.POPULATE_INTERACTION_COMPANY"

default_args={
    'email': ['pablo.diaz@moelis.com'],
    'email_on_failure': True,
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
    "retries": 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    DAG_ID,
    start_date = datetime(2024, 1, 1, tzinfo=local_tz),
    default_args = default_args,
    tags = ["ODS"],
    schedule = None,
    catchup = False,
) as dag:
    # Star process
    begin = EmptyOperator(task_id="begin")

    params = {
        'RUN_DATE':"2024-05-13 20:00:00.000"
    }

    SQL_CALL_SP = f"call {SNOWFLAKE_SP}('2024-05-13 20:00:00.000 +0000')"
    #SQL_CALL_SP = f"call {SNOWFLAKE_SP}(?, ?, ?)"
    #SQL_CALL_SP = f"call {SNOWFLAKE_SP}(%(feed_date)s, %(process_name)s, %(status)s)"

    populate_interaction_company = SnowflakeOperator(
        task_id = "populate_interaction_company",
        sql = SQL_CALL_SP,
        autocommit = True,
        #parameters = params,
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
