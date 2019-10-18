"""
dag for etl processing of shared streets segments
"""
from datetime import datetime, timedelta

import airflow
from airflow import dag
from airflow.operators.dummy_operator import dummyoperator

from airflow.operators.mssql_plugin import mssqloperator
from airflow.operators.dataframe_plugin import (
    geopandasuritoazuredatalakeoperator
)

default_args = {
    "owner": "airflow",
    "start_date":  datetime(2019, 4, 26),
    "email": ["pbotsqldbas@portlandoregon.gov"],
    "email_on_failure": true,
    "email_on_retry": false,
    "retries": 9,
    "retry_delay": timedelta(minutes=1),
}

dag = dag(
    dag_id="shst_segments_to_warehouse",
    default_args=default_args,
    schedule_interval=none
)

shst_segments_drop_external_table_task = mssqloperator(
    task_id='drop_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    if exists (
        select 1
        from sysobjects
        where name = 'external_shst_segments'
        and xtype='et'
    )
    drop external table etl.external_shst_segments
    """
)

shst_segments_create_external_table_task = mssqloperator(
    task_id='create_external_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql="""
    if not exists(
        select 1
        from sysobjects
        where name = 'external_shst_segment'
        and xtype='et'
    )
    create external table
        etl.external_shst_segment (
            geometry_id varchar(32) not null,
            reference_id varchar(32),
            from_intersection_id varchar(32),
            to_intersection_id varchar(32),
            direction varchar(10),
            shst_classification varchar(20),
            center_x decimal(24, 10),
            center_y decimal(24, 10),
            length decimal(24, 10),
            wkt varchar(max)
        )
    with (
        data_source = azuredatalakestorage,
        location = n'/transportation/mobility/dim/shst_segments.csv',
        file_format = pandascsvfileformat,
        reject_type = value,
        reject_value = 0
    )
    """
)

shst_segments_warehouse_update_task = mssqloperator(
    task_id="warehouse_update_shst_segments",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    update
        dim.shst_segment
    set
        last_seen = getdate()
    from
        etl.external_shst_segment as source
    where
        source.geometry_id = dim.shst_segment.geometry_id
    and source.reference_id = dim.shst_segment.reference_id
    """
)

shst_segments_warehouse_insert_task = mssqloperator(
    task_id="warehouse_insert_shst_segments",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    insert
        dim.shst_segment (
            geometry_id,
            reference_id,
            from_intersection_id,
            to_intersection_id,
            direction,
            shst_classification,
            center_x,
            center_y,
            length,
            wkt,
            first_seen,
            last_seen
        )
    select
        source.geometry_id,
        source.reference_id,
        source.from_intersection_id,
        source.to_intersection_id,
        source.direction,
        source.shst_classification,
        source.center_x,
        source.center_y,
        source.length,
        source.wkt,
        getdate(),
        getdate()
    from
        etl.external_shst_segment as source
    where
        not exists
        (
            select 1
            from
                dim.shst_segment as target
            where
                source.geometry_id = target.geometry_id
            and source.reference_id = target.reference_id
        )
    """
)

shst_segments_create_external_table_task << shst_segments_drop_external_table_task
shst_segments_warehouse_insert_task << shst_segments_create_external_table_task >> shst_segments_warehouse_update_task
