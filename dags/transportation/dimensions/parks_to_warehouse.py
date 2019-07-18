'''
DAG for ETL Processing of PDX GIS Open Data parks, from Metro
'''
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.dataframe_plugin import (
    GeoPandasUriToAzureDataLakeOperator
)

default_args = {
    'owner': 'airflow',
    "start_date":  datetime(2019, 4, 26),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
    'max_active_runs': 1
}

dag = DAG(
    dag_id='parks_to_warehouse',
    default_args=default_args,
    schedule_interval=None
)

parks_extract_warehouse_task = GeoPandasUriToAzureDataLakeOperator(
    task_id='parks_to_etl_datalake',
    dag=dag,
    uri='https://opendata.arcgis.com/datasets/9eef54196eaa4d12b54e9bc40e70ff09_35.geojson',
    local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parks-{{ ts_nodash }}.csv',
    remote_path='/transportation/mobility/etl/dim/parks.csv',
    columns=[
        'key',
        'hash',
        'name',
        'center_x',
        'center_y',
        'area'
    ],
    rename={
        'Name': 'name'
    }
)

parks_extract_datalake_task = GeoPandasUriToAzureDataLakeOperator(
    task_id='parks_to_dim_datalake',
    dag=dag,
    uri='https://opendata.arcgis.com/datasets/9eef54196eaa4d12b54e9bc40e70ff09_35.geojson',
    local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parks-{{ ts_nodash }}.csv',
    remote_path='/transportation/mobility/dim/parks.csv',
    columns=[
        'key',
        'hash',
        'name',
        'center_x',
        'center_y',
        'area',
        'wkt'
    ],
    rename={
        'Name': 'name'
    }
)

parks_drop_external_table_task = MsSqlOperator(
    task_id='drop_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    IF EXISTS (
        SELECT 1
        FROM sysobjects
        WHERE name = 'external_park'
        AND xtype='ET'
    )
    DROP EXTERNAL TABLE etl.external_park
    """
)

parks_create_external_table_task = MsSqlOperator(
    task_id='create_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    IF NOT EXISTS(
        SELECT 1
        FROM sysobjects
        WHERE name = 'external_park'
        AND xtype='ET'
    )
    CREATE EXTERNAL TABLE etl.external_park (
        [key] INT NOT NULL,
        [hash] VARCHAR(256),
        [name] VARCHAR(100),
        [center_x] [DECIMAL](24, 10),
        [center_y] [DECIMAL](24, 10),
        [area] [DECIMAL](24, 10)
    )
    WITH (
        DATA_SOURCE = [AzureDataLakeStorage],
        LOCATION = N'/transportation/mobility/etl/dim/parks.csv',
        FILE_FORMAT = [PandasCSVFileFormat],
        REJECT_TYPE = VALUE,
        REJECT_VALUE = 0
    )
    """
)


parks_warehouse_update_task = MsSqlOperator(
    task_id='warehouse_update_parks',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    UPDATE dim.park
    SET last_seen = GETDATE()
    FROM etl.external_park AS source
    WHERE source.[key] = dim.park.[key]
    AND source.hash = dim.park.hash
    '''
)

parks_warehouse_insert_task = MsSqlOperator(
    task_id='warehouse_insert_parks',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    INSERT dim.park (
        key,
        hash,
        name,
        center_x,
        center_y,
        area,
        first_seen,
        last_seen
    )
    SELECT source.[key],
    source.hash,
    source.name,
    source.center_x,
    source.center_y,
    source.area,
    GETDATE(),
    GETDATE()
    FROM etl.external_park AS source
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dim.park AS target
        WHERE source.[key] = target.[key]
        AND source.hash = target.hash
    )
    '''
)

parks_extract_warehouse_task >> parks_create_external_table_task << parks_drop_external_table_task
parks_warehouse_insert_task << parks_create_external_table_task >> parks_warehouse_update_task
