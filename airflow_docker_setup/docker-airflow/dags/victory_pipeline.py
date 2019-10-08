from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (DataQualityOperator, RunSQLFileOperator)
from helpers import SqlQueries, CreateTables, DeleteData


default_args = {
    'owner': 'Toly',
    'start_date': datetime(2019, 9, 1),
    #'end_date': datetime(2019, 10, 1),
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'retries' : 3,
    'catchup': False,
    'depends_on_past': False
}

dag = DAG('victory_redshift_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs = 3,
          catchup = False,
          schedule_interval = '@hourly' 
          
        )
schema = 'victory'
start_date = datetime.utcnow()

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_task = RunSQLFileOperator(
    task_id = 'create_tables_task',
    dag = dag,
    redshift_conn_id = 'redshift',
    sql_file = CreateTables.create_table_statement,
    database = 'dev' 
)

sessions_insert_task = RunSQLFileOperator(
    task_id = 'sessions_insert_task',
    dag = dag,
    redshift_conn_id = 'redshift',
    sql_file = SqlQueries.session_insert,
    database = 'dev' 
)

play_end_insert_task = RunSQLFileOperator(
    task_id = 'play_end_insert_task',
    dag = dag,
    redshift_conn_id = 'redshift',
    sql_file = SqlQueries.play_end_insert,
    database = 'dev' 
)

play_start_insert_task = RunSQLFileOperator(
    task_id = 'play_start_insert_task',
    dag = dag,
    redshift_conn_id = 'redshift',
    sql_file = SqlQueries.play_start_insert,
    database = 'dev' 
)

trim_raw_events = RunSQLFileOperator(
    task_id = 'trim_raw_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    sql_file = DeleteData.raw_table_delete,
    database = 'dev'
)

table_data_check = DataQualityOperator(
    task_id = 'table_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_names = [f'{schema}.play_end', f'{schema}.play_start', f'{schema}.sessions'],
    database = 'dev' 
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task >> sessions_insert_task >> [play_end_insert_task, 
play_start_insert_task] >> trim_raw_events >> table_data_check >> end_operator