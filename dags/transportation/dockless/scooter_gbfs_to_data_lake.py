'''
DAG for ETL Processing of Dockless Mobility Provider Data
'''
import hashlib
import json
import logging
import pathlib
import os

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from shapely.geometry import Point
from shapely.wkt import loads

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.mobility_plugin import GBFSFeedHook
from airflow.hooks.dataframe_plugin import AzureMsSqlDataFrameHook

from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator
from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityProviderSyncOperator,
    MobilityVehicleSyncOperator,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':  datetime(2019, 4, 26),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='scooter_gbfs_feeds_to_data_lake',
    default_args=default_args,
    catchup=False,
    schedule_interval=timedelta(minutes=2),
)

providers = ['lime', 'spin', 'bolt', 'shared', 'razor', 'bird']


def process_gbfs_to_data_lake(**kwargs):
    end_time = kwargs.get('execution_date')
    pace = timedelta(hours=48)
    start_time = end_time - pace

    # Create the hook
    hook_api = GBFSFeedHook(
        gbfs_feed_conn_id=kwargs['gbfs_feed_conn_id']
    )

    bikes = hook_api.get_free_bikes()

    bikes['provider'] = kwargs['provider']
    bikes['batch'] = kwargs['ts_nodash']
    bikes['seen'] = datetime.now()

    pathlib.Path(os.path.dirname(kwargs['templates_dict']['gbfs_local_path'])
                 ).mkdir(parents=True, exist_ok=True)

    bikes.to_csv(kwargs['templates_dict']['gbfs_local_path'], index=False)

    hook_data_lake = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['data_lake_conn_id'])

    hook_data_lake.upload_file(
        kwargs['templates_dict']['gbfs_local_path'], kwargs['templates_dict']['gbfs_remote_path'])

    os.remove(kwargs['templates_dict']['gbfs_local_path'])


trip_remote_files_delete_tasks = []
api_extract_tasks = []

# Extract data from providers and stage in tables
for provider in providers:
    gbfs_feed_conn_id = f'gbfs_feed_{provider}'

    gbfs_remote_path = f'/transportation/mobility/etl/gbfs/{provider}-{{{{ ts_nodash }}}}.csv'

    api_extract_tasks.append(PythonOperator(
        task_id=f'loading_{provider}_gbfs_feed',
        provide_context=True,
        python_callable=process_gbfs_to_data_lake,
        op_kwargs={
            'provider': provider,
            'gbfs_feed_conn_id': gbfs_feed_conn_id,
            'data_lake_conn_id': 'azure_data_lake_default',
        },
        templates_dict={
            'gbfs_local_path': f'/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-{{{{ ts_nodash }}}}.csv',
            'gbfs_remote_path': gbfs_remote_path,
        },
        dag=dag))
