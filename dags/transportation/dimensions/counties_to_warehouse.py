"""
DAG for ETL Processing of PDX GIS Open Data Counties, from Metro
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
    "owner": "airflow",
    "start_date":  datetime(2019, 4, 26),
    "email": ["pbotsqldbas@portlandoregon.gov"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 9,
    "retry_delay": timedelta(minutes=1),
    "concurrency": 1,
    "max_active_runs": 1
}

dag = DAG(
    dag_id="counties_to_warehouse",
    default_args=default_args,
    schedule_interval=None
)

counties_extract_warehouse_task = GeoPandasUriToAzureDataLakeOperator(
    task_id="counties_to_etl_datalake",
    dag=dag,
    uri='https://opendata.arcgis.com/datasets/65432a0067f949dd99f3ad0f51f11667_9.geojson',
    local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/counties-{{ ts_nodash }}.csv",
    remote_path='/transportation/mobility/etl/dim/counties.csv',
    columns=[
        'hash',
        'name',
        'center_x',
        'center_y',
        'area'
    ],
    rename={
        'COUNTY': 'name'
    }
)

counties_extract_datalake_task = GeoPandasUriToAzureDataLakeOperator(
    task_id="counties_to_dim_datalake",
    dag=dag,
    uri='https://opendata.arcgis.com/datasets/65432a0067f949dd99f3ad0f51f11667_9.geojson',
    local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/counties-{{ ts_nodash }}.csv",
    remote_path='/transportation/mobility/dim/counties.csv',
    columns=[
        'hash',
        'name',
        'center_x',
        'center_y',
        'area',
        'wkt'
    ],
    rename={
        'COUNTY': 'name'
    }
)

counties_drop_external_table_task = MsSqlOperator(
    task_id='drop_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    IF EXISTS (
        SELECT 1
        FROM sysobjects
        WHERE name = 'external_county'
        AND xtype='ET'
    )
    DROP EXTERNAL TABLE etl.external_county
    """
)

counties_create_external_table_task = MsSqlOperator(
    task_id='create_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    IF NOT EXISTS(
        SELECT 1
        FROM sysobjects
        WHERE name = 'external_county'
        AND xtype='ET'
    )
    CREATE EXTERNAL TABLE etl.external_county (
        [key] INT NOT NULL,
        [hash] VARCHAR(256),
        [name] VARCHAR(100),
        [center_x] [DECIMAL](24, 10),
        [center_y] [DECIMAL](24, 10),
        [area] [DECIMAL](24, 10)
    )
    WITH (
        DATA_SOURCE = [AzureDataLakeStorage],
        LOCATION = N'/transportation/mobility/etl/dim/counties.csv',
        FILE_FORMAT = [PandasCSVFileFormat],
        REJECT_TYPE = VALUE,
        REJECT_VALUE = 0
    )
    """
)

counties_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_counties",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    UPDATE dim.county
    SET last_seen = GETDATE()
    FROM etl.external_county AS source
    WHERE source.[key] = dim.county.[key]
    AND source.hash = dim.county.hash
    """
)

counties_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_counties",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT dim.county (
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
    FROM etl.external_county AS source
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dim.county AS target
        WHERE source.[key] = target.[key]
        AND source.hash = target.hash
    )
    """
)

counties_extract_warehouse_task >> counties_create_external_table_task << counties_drop_external_table_task
counties_warehouse_insert_task << counties_create_external_table_task >> counties_warehouse_update_task
