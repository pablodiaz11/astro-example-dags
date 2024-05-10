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
DAG_ID = "dag_import_snp_entities"
SNOWFLAKE_CONN_ID = "snow_devtest"

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
    tags = ["SnP"],
    schedule = None,
    catchup = False,
) as dag:
    # Star process
    begin = EmptyOperator(task_id="begin")

    import_snp_entities = BashOperator(
        task_id = "import_snp_entities",
        bash_command = "pwd",
    )

    end = EmptyOperator(task_id="end")

begin >> import_snp_entities >> end

