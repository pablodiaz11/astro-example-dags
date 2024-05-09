from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DAG_ID = "dag_call_snowflake_sp"
SNOWFLAKE_CONN_ID = "snow_devtest"
#SNOWFLAKE_SP = "ODS.META_DATA.POPULATE_INTERACTION_COMPANY"
SNOWFLAKE_SP = "STAGE.SP_PROCESS_RUN_END"

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["snowflake_sp"],
    schedule=None,
    catchup=False,
) as dag:
    # Star process
    begin = EmptyOperator(task_id="begin")

    params = {
        'feed_date':'2023-09-27',
        'process_name':'S&P API',
        'status':'Failed'
    }

    #SQL_CALL_SP = f"call {SNOWFLAKE_SP}('RUN_DATE' TIMESTAMP_TZ(9))"
    SQL_CALL_SP = "call {SNOWFLAKE_SP}(:feed_date, :process_name, :status)".format(SNOWFLAKE_SP=SNOWFLAKE_SP)

    populate_interaction_company = SnowflakeOperator(
        task_id="populate_interaction_company",
        sql=SQL_CALL_SP,
        parameters=params,
    )

    end = EmptyOperator(task_id="end")

    (
        begin >> populate_interaction_company >> end
    )
