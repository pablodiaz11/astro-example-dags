from __future__ import annotations
import os
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import pendulum
import pytz
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import requests
import json
import sys
import pandas as pd
import numpy as np

local_tz = pendulum.timezone("America/New_York")
dag_id = "dag_import_snp_entities"
cnx_snow_dsa_stage = 'snow_conn_test' #"cnx_snow_dsa_stage"
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

    #=======================================================
    #::::::::::::: Define credentials and services :::::::::
    #=======================================================
    
    #:::: SnP API ::::#
    var_snp_username = Variable.get('var_snp_username', default_var = None)
    var_snp_password = Variable.get('var_snp_password', default_var = None)
    var_snp_url = Variable.get('var_snp_url', default_var = None) # base url

    url_apikey = f'{var_snp_url}apikey'
    r = requests.post(url_apikey,data={'username': var_snp_username,'password': var_snp_password}, verify = True)

    if (r.status_code == 200):
        apikey = r.text
    else:
        sys.exit();
    
    #:::: Factset API ::::#
    authorization = (var_fs_username,var_fs_apikey)
    time_series_endpoint = var_fs_url
    headers = {'Accept': 'application/json','Content-Type': 'application/json'}
    
    #:::: Snowflake: Using Variable defined in Airflow ::::#
    # var_snow_user = Variable.get('var_snow_user', default_var = None)
    # var_snow_password = Variable.get('var_snow_password', default_var = None)
    # var_snow_account = Variable.get('var_snow_account', default_var = None)
    # var_snow_role = Variable.get('var_snow_role', default_var = None)
    # var_snow_warehouse = Variable.get('var_snow_warehouse', default_var = None)
    # var_snow_database = Variable.get('var_snow_database', default_var = None)
    # var_snow_schema_stg = Variable.get('var_snow_schema_stg', default_var = None)
    # var_snow_schema_gold = Variable.get('var_snow_schema_gold', default_var = None)
    # # Get conexion
    # conn_stg = snowflake.connector.connect(  
    #     user = var_snow_user,
    #     password = var_snow_password,
    #     account = var_snow_account,
    #     role = var_snow_role,
    #     warehouse = var_snow_warehouse,
    #     database = var_snow_database,
    #     schema = var_snow_schema_stg
    # )

    # conn_gold = snowflake.connector.connect(  
    #     user = var_snow_user,
    #     password = var_snow_password,
    #     account = var_snow_account,
    #     role = var_snow_role,
    #     warehouse = var_snow_warehouse,
    #     database = var_snow_database,
    #     schema = var_snow_schema_gold
    # )

    #:::: Using SnowflakeHook ::::#
    hook = SnowflakeHook(snowflake_conn_id = cnx_snow_dsa_stage)
    snow_dsa_conn = hook.get_conn()

    # Other way
    # snow_dsa_conn = BaseHook.get_connection(cnx_snow_dsa_stage)

    #=======================================================
    #:::::::::::::::::::: Define functions :::::::::::::::::
    #=======================================================

    # ::::: Function to verify if Key 'id' exists in a Dictionary ::::::
    def get_value(x):
        if type(x) == dict:
            if 'id' in x:
                return x['id']
            else:
                None
        else:
            None
            
    # ::::: Dump DF into Snowflake table
    def write_df_to_table(df, conn,table_name,database,schema):
        if df.empty == False:
            df.columns = df.columns.str.upper()

            write_pandas(
                        conn=conn,
                        df=df,
                        table_name=table_name,
                        database=database,
                        schema=schema
                    )
            print(f" {table_name} - Rows inserted: {df.shape[0]}")
        
    # :::: Truncate table :::::::
    def truncate_table(table_name, conn):
        query = f'truncate table {table_name}'
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        
    # :::::: Fix date format :::::::
    def fix_date_col(df,tz='UTC'):
        cols = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in cols:
            df[col] = df[col].dt.tz_localize(tz)
        return df

    # ::::::: call procedure ::::::::
    def call_procedure(procedure_name, conn):
        query = f'call {procedure_name}();'
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        print(f' {result[0][0]}')
        
    def call_procedure_list(procedure_query, conn):
        query = f'{procedure_query};'
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        return result

    def snp_import_entities(p_snp_username, p_snp_password, p_snp_url, p_conn_stg, p_conn_gold):
        snp_username = p_snp_username
        snp_password = p_snp_password
        snp_url = p_snp_url
        conn_stg = p_conn_stg
        conn_gold = p_conn_gold

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

            print(f'==> Process ended successful: {apikey}') # remove this, it just for testing

        except Exception as e:
            print('\n')
            print("Exception Name: {}".format(type(e).__name__))
            print("Exception Description: {}".format(e))

            sp_query_end = f"call STAGE.SP_PROCESS_RUN_END('{start_date}','{process_name}','Failed');" # change start_date by bdate
            r = call_procedure_list(sp_query_end,conn_stg)
            print(f'==> Process ended: {r[0][0]}') 
            print('')
        finally:
            snow_dsa_conn.close()

    # Start process
    begin = EmptyOperator(task_id="begin")

    # Import entities
    op_kwargs = {
        'p_snp_username': var_snp_username,
        'p_snp_password': var_snp_password,
        'p_snp_url': var_snp_url,
        'p_conn_stg': snow_dsa_conn, #conn_stg,
        'p_conn_gold': snow_dsa_conn #conn_gold
    }

    import_entities = PythonOperator(
        task_id = "import_entities",
        python_callable = snp_import_entities,
        op_kwargs = op_kwargs,
    )
    
    # End process
    end = EmptyOperator(task_id="end")

begin >> import_entities >> end

