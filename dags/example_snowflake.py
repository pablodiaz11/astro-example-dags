"""
Example use of Snowflake related operators.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

SNOWFLAKE_CONN_ID = "snow_conn_test"
SNOWFLAKE_SAMPLE_TABLE = "sample_table"

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_snowflake"


with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    schedule=None,
    catchup=False,
) as dag:
    # Star process
    start = EmptyOperator(task_id="start")
    # [START howto_operator_snowflake]
    create_table = SnowflakeOperator(
        task_id="create_table", 
        sql=CREATE_TABLE_SQL_STRING
    )

    def print_insert(SQL_INSERT_STATEMENT, SQL_LIST,SQL_MULTIPLE_STMTS):
        print(":: insert statement :::")
        print(SQL_INSERT_STATEMENT)
        print(":: insert list :::")
        print(SQL_LIST)
        print("::: SQL Multiple SRMTS")
        print(SQL_MULTIPLE_STMTS)

    print_insert = PythonOperator(
        task_id="print_insert",
        python_callable=print_insert,
        op_kwargs={'SQL_INSERT_STATEMENT': SQL_INSERT_STATEMENT, 'SQL_LIST':SQL_LIST, 'SQL_MULTIPLE_STMTS':SQL_MULTIPLE_STMTS}
    )
    insert_record = SnowflakeOperator(
        task_id="insert_record",
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 56},
    )

    insert_list = SnowflakeOperator(
        task_id="insert_list", 
        sql=SQL_LIST
    )

    # snowflake_op_sql_multiple_stmts = SnowflakeOperator(
    #     task_id="snowflake_op_sql_multiple_stmts",
    #     sql=SQL_MULTIPLE_STMTS,
    #     split_statements=True,
    # )

    # snowflake_op_template_file = SnowflakeOperator(
    #     task_id="snowflake_op_template_file",
    #     sql="example_snowflake_snowflake_op_template_file.sql",
    # )

    # # [END howto_operator_snowflake]

    # # [START howto_snowflake_sql_api_operator]
    # snowflake_sql_api_op_sql_multiple_stmt = SnowflakeSqlApiOperator(
    #     task_id="snowflake_op_sql_multiple_stmt",
    #     sql=SQL_MULTIPLE_STMTS,
    #     statement_count=len(SQL_LIST),
    # )
    # [END howto_snowflake_sql_api_operator]

    # End Process
    end = EmptyOperator(task_id="end")

    (
        start >> create_table >> print_insert >> insert_record >> insert_list >> end
        # >> [
        #     snowflake_op_with_params,
        #     snowflake_op_sql_list,
        #     snowflake_op_template_file,
        #     snowflake_op_sql_multiple_stmts,
        #     snowflake_sql_api_op_sql_multiple_stmt,
        # ]
    )


# from tests.system.utils import get_test_run  # noqa: E402

# # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
# test_run = get_test_run(dag)
