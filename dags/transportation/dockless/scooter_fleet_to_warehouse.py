"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityFleetToSqlExtractOperator
)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date":  datetime(2019, 4, 26),
    "email": ["pbotsqldbas@portlandoregon.gov"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 9,
    "retry_delay": timedelta(minutes=2),
    "concurrency": 1,
    "max_active_runs": 1,
    # "queue": "bash_queue",
    # "pool": "backfill",
    # "priority_weight": 10,
    # "end_date": datetime(2016, 1, 1),
}

dag = DAG(
    dag_id="scooter_fleet_to_warehouse",
    default_args=default_args,
    catchup=True,
    schedule_interval="@daily"
)

fleet_extract_task = MobilityFleetToSqlExtractOperator(
    task_id="build_fleet_extract",
    dag=dag,
    sql_conn_id="azure_sql_server_default",
    data_lake_conn_id="azure_data_lake_default",
    fleet_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv",
    fleet_remote_path="/transportation/mobility/etl/fleet_count/{{ ts_nodash }}.csv"
)

fleet_stage_task = MsSqlOperator(
    task_id="stage_fleet_extract",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO etl.stage_fleet_count (
        date_key
        ,provider_key
        ,time
        ,available
        ,reserved
        ,unavailable
        ,removed
        ,seen
        ,batch
    )
    SELECT
    date_key
    ,provider_key
    ,time
    ,a.count
    ,r.count
    ,u.count
    ,x.count
    ,seen
    ,batch
    FROM etl.extract_fleet_count AS e
    OUTER APPLY (
        SELECT COUNT(start_hash) AS count
        FROM fact.state AS f
        WHERE f.start_date_key = e.date_key
        AND f.provider_key = e.provider_key
        AND f.start_time <= e.time
        AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
        AND start_state = 'available'
    ) AS a
    OUTER APPLY (
        SELECT COUNT(start_hash) AS count
        FROM fact.state AS f
        WHERE f.start_date_key = e.date_key
        AND f.provider_key = e.provider_key
        AND f.start_time <= e.time
        AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
        AND start_state = 'reserved'
    ) AS r
    OUTER APPLY (
        SELECT COUNT(start_hash) AS count
        FROM fact.state AS f
        WHERE f.start_date_key = e.date_key
        AND f.provider_key = e.provider_key
        AND f.start_time <= e.time
        AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
        AND start_state = 'unavailable'
    ) AS u
    OUTER APPLY (
        SELECT COUNT(start_hash) AS count
        FROM fact.state AS f
        WHERE f.start_date_key = e.date_key
        AND f.provider_key = e.provider_key
        AND f.start_time <= e.time
        AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
        AND start_state = 'removed'
    ) AS x
    WHERE e.batch = '{{ ts_nodash }}'
    """
)

fleet_extract_task >> fleet_stage_task

fleet_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_fleet",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    UPDATE fact.fleet_count
    SET available = source.available,
    reserved = source.reserved,
    unavailable = source.unavailable,
    removed = source.removed,
    last_seen = source.seen
    FROM etl.stage_fleet_count AS source
    WHERE source.batch = '{{ ts_nodash }}'
    AND source.provider_key = fact.fleet_count.provider_key
    AND source.time = fact.fleet_count.time
    """
)
fleet_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_fleet",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT fact.fleet_count (
        date_key
        ,provider_key
        ,time
        ,available
        ,reserved
        ,unavailable
        ,removed
        ,first_seen
        ,last_seen
    )  
    SELECT source.date_key
    ,source.provider_key
    ,source.time
    ,source.available
    ,source.reserved
    ,source.unavailable
    ,source.removed
    ,source.seen
    ,source.seen
    FROM etl.stage_fleet_count AS source
    WHERE source.batch = '{{ ts_nodash }}'
    AND NOT EXISTS
    (
        SELECT 1
        FROM fact.fleet_count AS target
        WHERE source.provider_key = target.provider_key
        AND source.time = target.time
    )
    """
)

fleet_stage_task >> fleet_warehouse_insert_task
fleet_stage_task >> fleet_warehouse_update_task
