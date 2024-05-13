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
    var_fs_username = Variable.get('var_fs_username', default_var = None)
    var_fs_apikey = Variable.get('var_fs_apikey', default_var = None)
    var_fs_url = Variable.get('var_fs_url', default_var = None)

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

    snow_database = 'DSA'
    snow_schema_stg = 'STAGE'
    snow_schema_main = 'BLOOMBERG'

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
                        schema=schema,
                        use_logical_type= True
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

    def snp_import_entities(p_snp_username, p_snp_password, p_snp_url, p_snow_cnx_dsa):
        snp_username = p_snp_username
        snp_password = p_snp_password
        snp_url = p_snp_url
        snow_cnx_dsa = p_snow_cnx_dsa

        try:
            #---------------------------------------
            #:::::::::::::: Start Process ::::::::::
            #---------------------------------------

            start_date = '2024-05-06'
            end_date = datetime.now(tz=pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') #'2023-02-17'
            process_name = 'S&P API'
            status = 'Pending'

            sp_query = f"call {snow_schema_stg}.SP_PROCESS_RUN_START('{start_date}','{end_date}','{process_name}','{status}');"
            business_day = call_procedure_list(sp_query,snow_cnx_dsa)

            #-------------------------------------------
            #:::::::::::: Start data extraction ::::::::
            #-------------------------------------------

            # define api metadata
            namespace = "mlpd" # namespace
            service = "timeseries" # service
            
            # business_day = pd.bdate_range(start_date,end_date)

            for d in business_day:
                bdate = d[0].strftime('%Y-%m-%d')
            
                #----------------------------------------------
                #::::::::::::::::::::: Variables ::::::::::::::
                #----------------------------------------------
        
                # Price
                price_entity = 'Price'
                price_as_of_from = bdate
                price_as_of_to = bdate
                price_envelope = 'true'
                price_after = ''
                price_has_data = True
                price_limit = 10000
                price_table = f'SNP_{price_entity.upper()}'
                
                # Facility
                facility_entity = 'Facility'
                facility_table = f'SNP_{facility_entity.upper()}'
                
                # Organization
                organization_entity = 'Organization'
                organization_table = f'SNP_{organization_entity.upper()}'

                #============================================================================
                #:::::::::::::::::::::::::: CLEAN STAGE TABLES ::::::::::::::::::::::::::::::
                #============================================================================
                
                print(f'==> Process started: {bdate}')
                print(f' [Bronze]')

                truncate_table(price_table,snow_cnx_dsa)
                truncate_table(facility_table,snow_cnx_dsa)
                truncate_table(organization_table,snow_cnx_dsa)

                #============================================================================
                #:::::::::::::::::::::::::: GET PRICE ENTITY ::::::::::::::::::::::::::::::::
                #============================================================================

                lxid_all = []
                orgid_all = []

                while price_has_data:
                    price_path = namespace + '/'+ price_entity + '/' + service
                    price_query = f'?format=JSON&apikey={apikey}&from={price_as_of_from}&to={price_as_of_to}&envelope={price_envelope}&after={price_after}&limit={price_limit}'
                    price_req = var_snp_url + price_path + price_query

                    price_data = requests.get(price_req, verify = True)
                    price_json = price_data.json()

                    if price_data.status_code > 499:
                        print("Server-side error; handle by retrying")
                        price_has_data = False
                        sys.exit();

                    if price_data.status_code == 429:
                        print("Too many requests, handle by backing off")
                        print("for a period and retrying")
                        price_has_data = False
                        sys.exit();

                    if price_data.status_code > 399:
                        print("Client-side error; do not retry")
                        price_has_data = False
                        sys.exit();

                    if price_data.status_code == 200:
                        if 'after' in price_json.keys():
                            price_after = price_json['after']
                                                       
                            price_df = pd.DataFrame(price_json['timeseries'])
                            lxid_list = price_df['lxid'].unique().tolist()
                            
                            price_columns = ['id','asOf','asOfDateTime','facility','batchRun','batch','bidPrice','askPrice','midPrice',
                                            'depth','lxid','pricingStatus','contributionIds','displayDepth','analytics']
                                
                            for c in price_columns:
                                price_df[c] = price_df[c] if c in price_df.columns else None

                            price_df =  price_df[price_columns]

                            price_df['facility'] = price_df['facility'].apply(lambda x : x['id'] if 'id' in x else None)
                            price_df['batchRun'] = price_df['batchRun'].apply(lambda x : x['id'] if 'id' in x else None)
                            price_df['batch'] = price_df['batch'].apply(lambda x : x['id'] if 'id' in x else None)
                            price_df['analytics'] = price_df['analytics'].apply(lambda x : x['spreadToMaturity'] if 'spreadToMaturity' in x else None)
                            price_df['contributionIds'] = price_df['contributionIds'].apply(lambda x : ','.join(x))
                            price_df.rename(columns={'facility':'facilityId', 'batchRun':'batchRunId','batch':'batchId','analytics':'spreadToMaturity'}, inplace=True)
                            price_df['asOf'] = pd.to_datetime(price_df['asOf'].apply(lambda x : x.replace('Z','') if x is not np.NaN or x == '' else x))
                            price_df['asOfDateTime'] = pd.to_datetime(price_df['asOfDateTime'].apply(lambda x : x.replace('Z','').replace('T',' ') if x is not np.NaN or x == '' else x))

                            #============================================================================
                            #::::::::::::::::::::::: GET FACTSET FORMULA DATA :::::::::::::::::::::::::::
                            #============================================================================

                            # # Get FSYM_SECURITY_PERM_ID for each LoandXID Value from Factset Formula API
                            # fs_request = {
                            #     "data":{
                            #         "ids": lxid_list,
                            #         "formulas": ["FSYM_SECURITY_PERM_ID(\"SECURITY\")"]  }
                            #     }
                            # fs_post = json.dumps(fs_request)
                            # fs_response = requests.post(url = time_series_endpoint, data = fs_post, auth = authorization, headers = headers, verify= True )

                            # # Get response as Dataframe format
                            # formula_data = fs_response.json()['data']
                            # formula_df = pd.DataFrame(formula_data)

                            # # Add FsymSecurityPermId column into entity_data_df
                            # price_df['fsymSecurityPermId'] = price_df.merge(formula_df,how='left',left_on='lxid', right_on='requestId')['result']

                            #============================================================================
                            #:::::::::::::::::::::::::: GET FACILITY ENTITY :::::::::::::::::::::::::::::
                            #============================================================================

                            lxid_new = list(set(lxid_list) - set(lxid_all))
                            lxid_all.extend(lxid_new)

                            lxids = ','.join(lxid_new)
                            facility_path = namespace + '/'+ facility_entity + '/latest'
                            facility_query = f'?format=JSON&apikey={apikey}&lxid={lxids}'
                            facility_req = var_snp_url + facility_path + facility_query
                            facility_data = requests.get(facility_req, verify = True)

                            if facility_data.status_code == 200 and len(lxid_new) > 0:
                                facility_json = facility_data.json()
                                facility_df = pd.DataFrame(facility_json)

                                facility_df['issuer'] = facility_df['issuer'].apply(lambda x : x['id'] if 'id' in x else None) if 'issuer' in facility_df.columns else None
                                facility_df['leadAgent'] = facility_df['leadAgent'].apply(get_value) if 'leadAgent' in facility_df.columns else None
                                facility_df['adminAgent'] = facility_df['adminAgent'].apply(get_value) if 'adminAgent' in facility_df.columns else None
                                facility_df['docAgent'] = facility_df['docAgent'].apply(get_value) if 'docAgent' in facility_df.columns else None
                                facility_df['syndAgent'] = facility_df['syndAgent'].apply(get_value) if 'syndAgent' in facility_df.columns else None
                                facility_df.rename(columns={'issuer':'issuerId', 'leadAgent':'leadAgentId','adminAgent':'adminAgentId','docAgent':'docAgentId','syndAgent':'syndAgentId'},inplace=True)
                                facility_df['closeDate'] = pd.to_datetime(facility_df['closeDate'].apply(lambda x : x.replace('Z','') if x is not np.NaN or x == '' else x)) if 'closeDate' in facility_df.columns else None
                                facility_df['launchDate'] = pd.to_datetime(facility_df['launchDate'].apply(lambda x : x.replace('Z','') if x is not np.NaN or x == '' else x)) if 'launchDate' in facility_df.columns else None
                                facility_df['maturityDate'] = pd.to_datetime(facility_df['maturityDate'].apply(lambda x : x.replace('Z','') if x is not np.NaN or x == '' else x)) if 'maturityDate' in facility_df.columns else None
                                facility_df['createdTime'] = pd.to_datetime(facility_df['createdTime'].apply(lambda x : x.replace('Z','').replace('T',' ') if x is not np.NaN or x == '' else x)) if 'createdTime' in facility_df.columns else None
                                facility_df['modifiedTime'] = pd.to_datetime(facility_df['modifiedTime'].apply(lambda x : x.replace('Z','').replace('T',' ') if x is not np.NaN or x == '' else x)) if 'modifiedTime' in facility_df.columns else None
                                facility_df['defaultex'] = facility_df['defaultex'].astype('bool') if 'defaultex' in facility_df.columns else None
                                
                                facility_columns = ['id', 'lxid', 'country', 'orgCountry', 'countryCode', 'pmdId','issuerId', 'dealName', 'facilityType', 'loanxFacilityType','facilityStatus', 'facilityTypeCode', 
                                                    'facilityCategory', 'industry','initialAmount', 'initialSpread', 'maturityDate', 'currencyName','currency', 'sponsor', 'sponsorCode', 'closeDate', 'security',
                                                    'securityCode', 'leadAgentId', 'adminAgentId', 'spRating', 'industryId','sicId', 'sic', 'segment', 'segmentId', 'statusCode', 'status','cancelled', 'createdTime', 
                                                    'modifiedTime', 'term', 'rcTerm', 'tlaTerm','tlbTerm', 'tlcTerm', 'tldTerm', 'liborFloor', 'lien', 'covLite','comments', 'spOrganizationId', 'state', 'stateAbbr', 
                                                    'launchDate','commitmentFee', 'originalIssueDiscount', 'defaultex', 'docAgentId','syndAgentId', 'facilityFee', 'pmdTransId', 'consent', 'prAssignMin','prFee',
                                                    'institutionalFee','institutionalAssignment']
                                
                                for c in facility_columns:
                                    facility_df[c] = facility_df[c] if c in facility_df.columns else None

                                facility_df =  facility_df[facility_columns]

                                # Dump facility entity into Snowflake table
                                facility_df = fix_date_col(facility_df)
                                write_df_to_table(facility_df,snow_cnx_dsa,facility_table,snow_database,snow_schema_stg)

                            #============================================================================
                            #::::::::::::::::::::::: GET ORGANIZATION ENTITY ::::::::::::::::::::::::::::
                            #============================================================================

                            if facility_df.empty == False:
                                orgid_list = facility_df['ISSUERID'].unique().tolist()
                                orgid_new = list(set(orgid_list) - set(orgid_all))
                                orgid_all.extend(orgid_new)

                                orgids = ','.join(orgid_new)
                                organization_path = namespace + '/'+ organization_entity + '/latest'
                                organization_query = f'?format=JSON&apikey={apikey}&id={orgids}'
                                organization_req = var_snp_url + organization_path + organization_query
                                organization_data = requests.get(organization_req, verify = True)

                                if organization_data.status_code == 200:
                                    organization_json = organization_data.json()
                                    organization_df = pd.DataFrame(organization_json)
                                    
                                    organization_columns = ['id', 'name', 'ticker', 'sectorLevel2']
                                    
                                    for c in organization_columns:
                                        organization_df[c] = organization_df[c] if c in organization_df.columns else None

                                    organization_df =  organization_df[organization_columns]

                                    # Dump organization entity into Snowflake table
                                    organization_df = fix_date_col(organization_df)
                                    write_df_to_table(organization_df,snow_cnx_dsa,organization_table,snow_database,snow_schema_stg)

                            # Dump price entity into Snowflake table
                            price_df = fix_date_col(price_df)
                            write_df_to_table(price_df,snow_cnx_dsa,price_table,snow_database,snow_schema_stg)

                        else:
                            price_has_data = False
                    else:
                        price_has_data = False
                        sp_query_end = f"call {snow_schema_stg}.SP_PROCESS_RUN_END('{bdate}','{process_name}','Failed');"
                        r = call_procedure_list(sp_query_end,snow_cnx_dsa)
                        print(f'==> Process ended: {r[0][0]}') 
                        print('')
                        sys.exit()

                #============================================================================
                #:::::::::::::::::::::::::: MERGE INTO GOLD STAGE :::::::::::::::::::::::::::
                #============================================================================

                print(f' [Gold]')
                if len(lxid_all) > 0:
                    call_procedure(f'{snow_schema_main}.sp_snp_upsert_price',snow_cnx_dsa)
                    call_procedure(f'{snow_schema_main}.sp_snp_upsert_organization',snow_cnx_dsa)
                    call_procedure(f'{snow_schema_main}.sp_snp_upsert_facility',snow_cnx_dsa)
                    
                    sp_query_end = f"call {snow_schema_stg}.SP_PROCESS_RUN_END('{bdate}','{process_name}','Success');"
                    r = call_procedure_list(sp_query_end,snow_cnx_dsa)
                    print(f'==> Process ended: {r[0][0]}') 
                    print()
                else:
                    print(f' There no data yet for {bdate}, try later.')
                    sp_query_end = f"call {snow_schema_stg}.SP_PROCESS_RUN_END('{bdate}','{process_name}','Pending');"
                    r = call_procedure_list(sp_query_end,snow_cnx_dsa)
                    print(f'==> Process ended: {r[0][0]}')
                    print()
                
            #-------------------------------------------------------
            #:::::::::::: Pull additionals entities ::::::::::::::::
            #-------------------------------------------------------
            
            print('==> Pulling additionals Entities')
            
            #::::::: Pull Batch entity :::::::
            
            batch_entity = 'Batch'
            service = 'latest'
            batch_table = f'SNP_{batch_entity.upper()}'
            
            batch_path = namespace + '/'+ batch_entity + '/' + service
            batch_query = f'?format=JSON&apikey={apikey}'
            batch_req = var_snp_url + batch_path + batch_query
            batch_data = requests.get(batch_req, verify = True)

            if batch_data.status_code == 200:
                batch_json = batch_data.json()
                batch_df = pd.DataFrame(batch_json)
                
                batch_columns = ['id','batchRunEndSla','batchRunStartSla','year','dayOfMonth','dayOfWeek','seconds','minutes',
                                'hours','month','timeZone','entities','name']
                                    
                for c in batch_columns:
                    batch_df[c] = batch_df[c] if c in batch_df.columns else None

                batch_df =  batch_df[batch_columns]

                batch_df['entities'] = batch_df['entities'].apply(lambda x : ','.join(x))

                # Dump Batch entity into Snowflake table
                batch_df = fix_date_col(batch_df)
                truncate_table(batch_table,snow_cnx_dsa)
                write_df_to_table(batch_df,snow_cnx_dsa,batch_table,snow_database,snow_schema_stg)
                call_procedure(f'{snow_schema_main}.sp_snp_upsert_batch',snow_cnx_dsa)

            #:::::::: Pull Batchrun entity ::::::::::
            
            batchrun_entity = 'BatchRun'
            service = 'latest'
            batchrun_table = f'SNP_{batchrun_entity.upper()}'
            
            batchrun_path = namespace + '/'+ batchrun_entity + '/' + service
            batchrun_query = f'?format=JSON&apikey={apikey}'
            batchrun_req = var_snp_url + batchrun_path + batchrun_query
            batchrun_data = requests.get(batchrun_req, verify = True)

            if batchrun_data.status_code == 200:
                batchrun_json = batchrun_data.json()
                batchrun_df = pd.DataFrame(batchrun_json)
                
                batchrun_columns = ['id','batch','asOf','runDate','version','recordCount','statusIdentifier']
                                    
                for c in batchrun_columns:
                    batchrun_df[c] = batchrun_df[c] if c in batchrun_df.columns else None

                batchrun_df =  batchrun_df[batchrun_columns]

                batchrun_df['batch'] = batchrun_df['batch'].apply(get_value)
                batchrun_df.rename(columns={'batch':'batchId'}, inplace=True)
                batchrun_df['asOf'] = pd.to_datetime(batchrun_df['asOf'].apply(lambda x : x.replace('Z','').replace('T',' ') if x is not np.NaN or x == '' else x))
                batchrun_df['runDate'] = pd.to_datetime(batchrun_df['runDate'].apply(lambda x : x.replace('Z','').replace('T',' ') if x is not np.NaN or x == '' else x))

                # Dump Batchrun entity into Snowflake table
                batchrun_df = fix_date_col(batchrun_df)
                truncate_table(batchrun_table,snow_cnx_dsa)
                write_df_to_table(batchrun_df,snow_cnx_dsa,batchrun_table,snow_database,snow_schema_stg)
                call_procedure(f'{snow_schema_main}.sp_snp_upsert_batchrun',snow_cnx_dsa)

            #::::::::::: Pull Agent entity :::::::::::::
            
            agent_entity = 'Agent'
            service = 'latest'
            agent_table = f'SNP_{agent_entity.upper()}'
            
            agent_path = namespace + '/'+ agent_entity + '/' + service
            agent_query = f'?format=JSON&apikey={apikey}'
            agent_req = var_snp_url + agent_path + agent_query
            agent_data = requests.get(agent_req, verify = True)

            if agent_data.status_code == 200:
                agent_json = agent_data.json()
                agent_df = pd.DataFrame(agent_json)
                
                agent_columns = ['id','name']
                                    
                for c in agent_columns:
                    agent_df[c] = agent_df[c] if c in agent_df.columns else None

                agent_df =  agent_df[agent_columns]

                # Dump Agent entity into Snowflake table
                agent_df = fix_date_col(agent_df)
                truncate_table(agent_table,snow_cnx_dsa)
                write_df_to_table(agent_df,snow_cnx_dsa,agent_table,snow_database,snow_schema_stg)
                call_procedure(f'{snow_schema_main}.sp_snp_upsert_agent',snow_cnx_dsa)
            
            print('')

        except Exception as e:
            print('\n')
            print("Exception Name: {}".format(type(e).__name__))
            print("Exception Description: {}".format(e))

            sp_query_end = f"call {snow_schema_stg}.SP_PROCESS_RUN_END('{bdate}','{process_name}','Failed');"
            r = call_procedure_list(sp_query_end,snow_cnx_dsa)
            print(f'==> Process ended: {r[0][0]}') 
            print('')
            raise e
            print('')
        finally:
            print(f'==> Closing Snowflake connection.') 
            snow_dsa_conn.close()

    # Start process
    begin = EmptyOperator(task_id="begin")

    # Import entities
    op_kwargs = {
        'p_snp_username': var_snp_username,
        'p_snp_password': var_snp_password,
        'p_snp_url': var_snp_url,
        'p_snow_cnx_dsa': snow_dsa_conn
    }

    import_entities = PythonOperator(
        task_id = "import_entities",
        python_callable = snp_import_entities,
        op_kwargs = op_kwargs,
    )
    
    # End process
    end = EmptyOperator(task_id="end")

begin >> import_entities >> end

