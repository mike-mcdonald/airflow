'''
DAG for ETL Processing of Dockless Mobility Provider Data
'''
import hashlib
import json
import logging
import pathlib
import os
import uuid

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from shapely.geometry import Point
from shapely.wkb import loads

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.common_plugin import PgSqlDataFrameHook
from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.dataframe_plugin import AzureMsSqlDataFrameHook
from airflow.hooks.mobility_plugin import SharedStreetsAPIHook

from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator
from airflow.operators.mssql_plugin import MsSqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2018, 4, 26),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(seconds=10),
    'concurrency': 1,
    'max_active_runs': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='scooter_pilot_1_vehicles_to_warehouse',
    default_args=default_args,
    max_active_runs=3,
)


def process_vehicles_to_data_lake(**kwargs):
    # Create the hook
    hook_pgsql = PgSqlDataFrameHook(
        pgsql_conn_id='pgsql_scooter_pilot_1'
    )

    # Get trips as a GeoDataFrame
    vehicles = hook_pgsql.read_sql('''
        select
            v.name as vehicle_id
        from
            dim.vehicle as v
    ''')

    vehicles['device_id'] = vehicles.apply(lambda x: str(uuid.uuid4()), axis=1)
    vehicles['vehicle_type'] = 'scooter'

    vehicles.to_csv(kwargs.get('templates_dict').get(
        'local_path'), index=False)

    hook_datalake = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs.get('azure_datalake_conn_id'))

    hook_datalake.upload_file(kwargs.get('templates_dict').get(
        'local_path'), kwargs.get('templates_dict').get('remote_path'))

    os.remove(kwargs.get('templates_dict').get('local_path'))


extract_data_lake_task = PythonOperator(
    task_id='extract_routes_to_data_lake',
    dag=dag,
    provide_context=True,
    python_callable=extract_shst_hits_datalake,
    templates_dict={
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/mobility/etl/pilot_1/vehicles/{{ ts_nodash }}.csv',
    },
    op_kwargs={
        'azure_datalake_conn_id': 'azure_data_lake_default'
    },
)

insert_pilot_1_vehicle_task = MsSqlOperator(
    task_id=f'insert_pilot_1_vehicles',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    pool='scooter_azure_sql_server',
    sql='''
    insert into
        dim.vehicle (
            device_id,
            vehicle_id,
            vehicle_type
        )
    select distinct
        device_id,
        vehicle_id,
        vehicle_type
    from
        etl.external_pilot_1_vehicle as e
    where
        not exists (
            select
                1
            from
                dim.vehicle as v
            where
                v.vehicle_id = e.vehicle_id
        )
    ''')

extract_data_lake_task >> insert_pilot_1_vehicle_task
