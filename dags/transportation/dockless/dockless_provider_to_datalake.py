"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from configparser import ConfigParser
import time
import pytz
import os
import logging
import json

from airflow.operators.mobility_plugin import MobilityTripsToAzureDataLakeOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 30),
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
    schedule_interval='@hourly'
)

providers = ['pdx']

task1 = DummyOperator(
    task_id="provider_dummy",
    dag=dag
)

for provider in providers:
    provider_to_datalake_task = MobilityTripsToAzureDataLakeOperator(
        task_id=f"loading_{provider}_data",
        provide_context=True,
        mobility_provider_conn_id=f"mobility_provider_{provider}",
        mobility_provider_token_conn_id=f"mobility_provider_{provider}_token",
        azure_data_lake_conn_id="azure_data_lake_default",
        dag=dag)
    provider_to_datalake_task.set_upstream(task1)
