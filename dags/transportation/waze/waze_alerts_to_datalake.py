'''
DAG for ETL Processing of Waze alerts
'''
from datetime import datetime, timedelta

import airflow
from airflow import DAG

from airflow.operators.waze_plugin import WazeAlertsToDataLakeOperator

default_args = {
    'owner': 'airflow',
    'start_date':  datetime(2019, 8, 7),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
    'max_active_runs': 1
}

dag = DAG(
    dag_id='waze_alerts_to_datalake',
    catchup=False,
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

alerts_to_datalake_task = WazeAlertsToDataLakeOperator(
    task_id="alerts_to_datalake",
    dag=dag,
    waze_conn_id='waze_portland',
    local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
    remote_path='/transportation/waze/etl/alert/{{ ts_nodash }}.csv',
)
