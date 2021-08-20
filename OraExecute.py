from __future__ import print_function    
import airflow                        
from datetime import datetime, timedelta     
from mssql_to_oracle import MsSqlToOracleTransfer               
from airflow.models import Variable      

args = {                                                  
  'owner': 'YELNAR',                                       
  'start_date': datetime(2021,8, 16),   
  'provide_context': True               
} 

with airflow.DAG( 
  'merge_log_staging', 
  schedule_interval='@daily',
  dagrun_timeout=timedelta(minutes=120), 
  default_args=args,
  max_active_runs=1    
) as dag:               
  extract_web_log = MsSqlToOracleTransfer(  
    task_id= 'mssqlToOracle',             
    dag=dag, 
    mssql_conn_id = 'mssql_default',   
    oracle_conn_id = 'orcl_conn_id',  
    sql= 'include/MssqlToOracle.sql',  
    oracle_table = 'DWH_STAGE2.ZZB0067$FACT_SCHEDULE' 
  )
