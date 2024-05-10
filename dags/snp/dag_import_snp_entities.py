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

    first_name = 'Pablo'
    last_name = 'Diaz'

    op_kwargs = {
        'vFirstName': first_name,
        'vLastName': last_name
    }

    def hello(vFirstName, vLastName):
        say = 'Hello'
        first_name = vFirstName
        last_name = vLastName
        message = f"{say} {first_name} {last_name}!"
        print(message)

    import_snp_entities = PythonOperator(
        task_id = "import_snp_entities",
        python_callable = hello,
        op_kwargs = op_kwargs,
    )

    end = EmptyOperator(task_id="end")

begin >> import_snp_entities >> end

