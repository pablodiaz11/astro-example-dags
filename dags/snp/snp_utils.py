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
    print(f' {result[0][0]}')
    
def call_procedure_list(procedure_query, conn):
    query = f'{procedure_query};'
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    return result