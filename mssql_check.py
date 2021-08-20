from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.mssql_operator import MsSqlOperator
from airflow import DAG

default_args = {
        'owner': 'yelnar',
        }

dag = DAG('mssql_task',
        default_args=default_args,
        schedule_interval = '@monthly',
        start_date = datetime(2021, 8, 1),
        catchup=False  
        )

 
conn_orc = MsSqlOperator(
        task_id = "mssql_conn_check",
        mssql_conn_id="mssql_default",
        sql='select count(1) FROM [TT].[m].[table] fa with (NOLOCK) where [shift_startdate_local_id]%1000=0;',
        autocommit = True,
        database = 'TT',
        dag= dag
    )
       
