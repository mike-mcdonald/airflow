"""
DAG for ETL Processing of PDX GIS Open Data zipcodes, from Metro
"""
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
    dag_id='zipcodes_to_warehouse',
    default_args=default_args,
    schedule_interval=None
)

zipcodes_extract_warehouse_task = GeoPandasUriToAzureDataLakeOperator(
    task_id='zipcodes_to_etl_datalake',
    dag=dag,
    uri='https://opendata.arcgis.com/datasets/71fa9b7ab6a040939d7c0339287fa436_1.geojson',
    local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/zipcodes-{{ ts_nodash }}.csv',
    remote_path='/transportation/mobility/etl/dim/zipcodes.csv',
    columns=[
        'key',
        'hash',
        'name',
        'center_x',
        'center_y',
        'area'
    ]
)

zipcodes_extract_datalake_task = GeoPandasUriToAzureDataLakeOperator(
    task_id='zipcodes_to_dim_datalake',
    dag=dag,
    uri='https://opendata.arcgis.com/datasets/71fa9b7ab6a040939d7c0339287fa436_1.geojson',
    local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/zipcodes-{{ ts_nodash }}.csv',
    remote_path='/transportation/mobility/dim/zipcodes.csv',
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
        'Zip Code': 'name'
    }
)

zipcodes_drop_external_table_task = MsSqlOperator(
    task_id='drop_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    IF EXISTS (
        SELECT 1
        FROM sysobjects
        WHERE name = 'external_zipcode'
        AND xtype='ET'
    )
    DROP EXTERNAL TABLE etl.external_zipcode
    """
)

zipcodes_create_external_table_task = MsSqlOperator(
    task_id='create_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    IF NOT EXISTS(
        SELECT 1
        FROM sysobjects
        WHERE name = 'external_zipcode'
        AND xtype='ET'
    )
    CREATE EXTERNAL TABLE etl.external_zipcode (
        [key] INT NOT NULL,
        [hash] VARCHAR(256),
        [name] VARCHAR(100),
        [center_x] [DECIMAL](24, 10),
        [center_y] [DECIMAL](24, 10),
        [area] [DECIMAL](24, 10)
    )
    WITH (
        DATA_SOURCE = [AzureDataLakeStorage],
        LOCATION = N'/transportation/mobility/etl/dim/zipcodes.csv',
        FILE_FORMAT = [PandasCSVFileFormat],
        REJECT_TYPE = VALUE,
        REJECT_VALUE = 0
    )
    """
)

zipcodes_warehouse_update_task = MsSqlOperator(
    task_id='warehouse_update_zipcodes',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    UPDATE dim.zipcode
    SET last_seen = GETDATE()
    FROM etl.external_zipcode AS source
    WHERE source.[key] = dim.zipcode.[key]
    AND source.hash = dim.zipcode.hash
    """
)

zipcodes_warehouse_insert_task = MsSqlOperator(
    task_id='warehouse_insert_zipcodes',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    INSERT dim.zipcode (
        [key],
        [hash],
        [name],
        [center_x],
        [center_y],
        [area],
        [first_seen],
        [last_seen]
    )
    SELECT source.[key],
    source.hash,
    source.name,
    source.center_x,
    source.center_y,
    source.area,
    GETDATE(),
    GETDATE()
    FROM etl.external_zipcode AS source
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dim.zipcode AS target
        WHERE source.[key] = target.[key]
        AND source.hash = target.hash
    )
    """
)

zipcodes_extract_warehouse_task >> zipcodes_create_external_table_task << zipcodes_drop_external_table_task
zipcodes_warehouse_insert_task << zipcodes_create_external_table_task >> zipcodes_warehouse_update_task
