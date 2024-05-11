from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum

local_tz = pendulum.timezone("America/New_York")
dag_id = "dag_import_snp_entities"
cnx_snow_dsa_stage = "cnx_snow_dsa_stage"
cnx_snow_dsa_bloomberg = "cnx_snow_dsa_bloomberg"

default_args={
    'email': ['pablo.diaz@moelis.com'],
    'email_on_failure': True,
    "snowflake_conn_id": cnx_snow_dsa_stage,
    "retries": 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id,
    start_date = datetime(2024, 1, 1, tzinfo=local_tz),
    default_args = default_args,
    tags = ["SnP"],
    schedule = None,
    catchup = False,
) as dag:
    # Star process
    begin = EmptyOperator(task_id="begin")

    # SnP API
    var_snp_username = Variable.get('var_snp_username', default_var = None)
    var_snp_password = Variable.get('var_snp_password', default_var = None)
    var_snp_url = Variable.get('var_snp_url', default_var = None)
    # Factset API
    var_fs_username = Variable.get('var_fs_username', default_var = None)
    var_fs_apikey = Variable.get('var_fs_apikey', default_var = None)
    var_fs_url = Variable.get('var_fs_url', default_var = None)

    op_kwargs = {
        'p_snp_username': var_snp_username,
        'p_snp_password': var_snp_password,
        'p_snp_url': var_snp_url
    }

    def snp_import_entities(p_snp_username, p_snp_password, p_snp_url):
        snp_username = p_snp_username
        snp_password = p_snp_password
        snp_url = p_snp_url
        message = f"value from variable: {snp_username} | {snp_password} | {snp_url}!"
        print(message)

    import_snp_entities = PythonOperator(
        task_id = "import_snp_entities",
        python_callable = snp_import_entities,
        op_kwargs = op_kwargs,
    )

    end = EmptyOperator(task_id="end")

begin >> import_snp_entities >> end

