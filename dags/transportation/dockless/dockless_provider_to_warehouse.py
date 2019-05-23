"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.mobility_plugin import MobilityTripsToSqlTablesOperator, MobilityEventsToSqlTableOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':  datetime(2019, 4, 26),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'concurrency': 50
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='dockless-provider-to-datalake',
    default_args=default_args,
    catchup=True,
    schedule_interval='@hourly',
)

providers = ['lime', 'clevr', 'spin', 'jump', 'razor', 'shared', 'bolt']

task1 = DummyOperator(
    task_id="provider_extract_start",
    dag=dag
)

task2 = DummyOperator(
    task_id="provider_extract_complete",
    dag=dag
)

# Extract data from providers and stage in tables
for provider in providers:
    trip_extract_task = MobilityTripsToSqlTablesOperator(
        task_id=f"loading_{provider}_trips",
        provide_context=True,
        mobility_provider_conn_id=f"mobility_provider_{provider}",
        mobility_provider_token_conn_id=f"mobility_provider_{provider}_token",
        sql_conn_id="azure_sql_server_default",
        dag=dag)

    event_extract_task = MobilityEventsToSqlTableOperator(
        task_id=f"loading_{provider}_events",
        provide_context=True,
        mobility_provider_conn_id=f"mobility_provider_{provider}",
        mobility_provider_token_conn_id=f"mobility_provider_{provider}_token",
        sql_conn_id="azure_sql_server_default",
        dag=dag)

    trip_extract_task.set_upstream(task1)
    event_extract_task.set_upstream(task1)

    trip_extract_task.set_downstream(task2)
    event_extract_task.set_downstream(task2)

task3 = DummyOperator(
    task_id="provider_staging_complete",
    dag=dag
)

# Run SQL scripts to transform extract data into staged facts
trips_stage_task = MobilityTripsToSqlStageOperator(
    task_id=f"staging_trips",
    provide_context=True,
    sql_conn_id="azure_sql_server_default",
    dag=dag)

event_stage_task = MobilityEventsToSqlStageOperator(
    task_id=f"staging_states",
    provide_context=True,
    sql_conn_id="azure_sql_server_default",
    dag=dag)

trips_stage_task.set_upstream(task2)
event_stage_task.set_upstream(task2)

trips_stage_task.set_downstream(task3)
event_stage_task.set_downstream(task3)

# Run SQL scripts to load staged data to warehouse