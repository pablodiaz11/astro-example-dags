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
import pytz

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
        'p_snp_url': var_snp_url,
        'p_cnx_snow_dsa_stage': cnx_snow_dsa_stage
    }

    def call_procedure_list(procedure_query, conn):
        query = f'{procedure_query};'
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        return result

    def snp_import_entities(p_snp_username, p_snp_password, p_snp_url, p_cnx_snow_dsa_stage):
        snp_username = p_snp_username
        snp_password = p_snp_password
        snp_url = p_snp_url
        conn_stg = p_cnx_snow_dsa_stage

        try:
            #---------------------------------------
            #:::::::::::::: Start Process ::::::::::
            #---------------------------------------

            start_date = '2024-05-06'
            end_date = datetime.now(tz=pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') #'2023-02-17'
            process_name = 'S&P API'
            status = 'Pending'

            sp_query = f"call STAGE.SP_PROCESS_RUN_START('{start_date}','{end_date}','{process_name}','{status}');"
            business_day = call_procedure_list(sp_query,conn_stg)

            print(f'==> Process ended successful') 

        except Exception as e:
            print('\n')
            print("Exception Name: {}".format(type(e).__name__))
            print("Exception Description: {}".format(e))

            sp_query_end = f"call STAGE.SP_PROCESS_RUN_END('{start_date}','{process_name}','Failed');" # change start_date by bdate
            r = call_procedure_list(sp_query_end,conn_stg)
            print(f'==> Process ended: {r[0][0]}') 
            print('')

    # Start process
    begin = EmptyOperator(task_id="begin")

    # Import entities
    import_snp_entities = PythonOperator(
        task_id = "import_snp_entities",
        python_callable = snp_import_entities,
        op_kwargs = op_kwargs,
    )
    
    # End process
    end = EmptyOperator(task_id="end")

begin >> import_snp_entities >> end

