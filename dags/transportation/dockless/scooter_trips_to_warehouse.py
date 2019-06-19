"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityTripsToSqlExtractOperator,
    MobilityTripsToSqlWarehouseOperator,
    MobilityProviderSyncOperator,
    MobilityVehicleSyncOperator
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
    dag_id="scooter_trips_to_warehouse",
    default_args=default_args,
    catchup=True,
    schedule_interval="@hourly",
)

providers = ["lime", "spin", "bolt"]

task1 = DummyOperator(
    task_id="provider_extract_start",
    dag=dag
)

task2 = DummyOperator(
    task_id="provider_extract_complete",
    dag=dag
)

clean_extract_task = MsSqlOperator(
    task_id="clean_extract_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.extract_trip WHERE batch = '{{ ts_nodash }}'
    DELETE FROM etl.extract_segment_hit WHERE batch = '{{ ts_nodash }}'
    """
)
clean_extract_task.set_downstream(task1)

# Extract data from providers and stage in tables
for provider in providers:
    mobility_provider_conn_id = f"mobility_provider_{provider}"
    mobility_provider_token_conn_id = f"mobility_provider_{provider}_token"

    trip_extract_task = MobilityTripsToSqlExtractOperator(
        task_id=f"loading_{provider}_trips",
        provide_context=True,
        mobility_provider_conn_id=mobility_provider_conn_id,
        mobility_provider_token_conn_id=mobility_provider_token_conn_id,
        sql_conn_id="azure_sql_server_default",
        dag=dag)

    trip_extract_task.set_upstream(task1)
    trip_extract_task.set_downstream(task2)


clean_stage_task = MsSqlOperator(
    task_id="clean_stage_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_trip WHERE batch = '{{ ts_nodash }}'
    DELETE FROM etl.stage_segment_hit WHERE batch = '{{ ts_nodash }}'
    """
)

task2 >> clean_stage_task

provider_sync_task = MobilityProviderSyncOperator(
    task_id="provider_sync",
    source_table="etl.extract_trip",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)

task2 >> provider_sync_task

vehicle_sync_task = MobilityVehicleSyncOperator(
    task_id="vehicle_sync",
    source_table="etl.extract_trip",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)

task2 >> vehicle_sync_task

trip_stage_task = MsSqlOperator(
    task_id="stage_trips",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO [etl].[stage_trip] (
        [provider_key]
        ,[vehicle_key]
        ,[propulsion_type]
        ,[trip_id]
        ,[origin]
        ,[destination]
        ,[start_time]
        ,[start_date_key]
        ,[end_time]
        ,[end_date_key]
        ,[distance]
        ,[duration]
        ,[accuracy]
        ,[standard_cost]
        ,[actual_cost]
        ,[parking_verification_url]
        ,[seen]
        ,[batch]
    )
    SELECT
    p.[key]
    ,v.[key]
    ,[propulsion_type]
    ,[trip_id]
    ,[origin]
    ,[destination]
    ,[start_time]
    ,[start_date_key]
    ,[end_time]
    ,[end_date_key]
    ,[distance]
    ,[duration]
    ,[accuracy]
    ,[standard_cost]
    ,[actual_cost]
    ,[parking_verification_url]
    ,[seen]
    ,[batch]
    FROM etl.extract_trip AS source
    LEFT JOIN dim.provider AS p ON p.provider_id = source.provider_id
    LEFT JOIN dim.vehicle as v ON (
        v.vehicle_id = source.vehicle_id
        AND v.device_id = source.device_id
    )
    WHERE batch = '{{ ts_nodash }}'
    """
)

clean_stage_task >> trip_stage_task
provider_sync_task >> trip_stage_task
vehicle_sync_task >> trip_stage_task

trip_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_trip",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    UPDATE fact.trip
    SET last_seen = source.seen
    FROM etl.stage_trip AS source
    WHERE source.trip_id = fact.trip.trip_id
    AND source.batch = '{{ ts_nodash }}'
    """
)

trip_stage_task >> trip_warehouse_update_task

trip_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_trip",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO [fact].[trip] (
        [trip_id]
        ,[provider_key]
        ,[vehicle_key]
        ,[propulsion_type]
        ,[origin]
        ,[destination]
        ,[start_time]
        ,[start_date_key]
        ,[end_time]
        ,[end_date_key]
        ,[distance]
        ,[duration]
        ,[accuracy]
        ,[standard_cost]
        ,[actual_cost]
        ,[parking_verification_url]
    )
    SELECT
    [trip_id]
    ,[provider_key]
    ,[vehicle_key]
    ,[propulsion_type]
    ,[origin]
    ,[destination]
    ,[start_time]
    ,[start_date_key]
    ,[end_time]
    ,[end_date_key]
    ,[distance]
    ,[duration]
    ,[accuracy]
    ,[standard_cost]
    ,[actual_cost]
    ,[parking_verification_url]
    FROM [etl].[stage_trip] AS source
    WHERE batch = '{{ ts_nodash }}'
    AND NOT EXISTS (
        SELECT 1
        FROM fact.trip AS AS target
        WHERE target.trip_id = source.trip_id
    )
    """
)

trip_stage_task >> trip_warehouse_insert_task

route_stage_task = MsSqlOperator(
    task_id="stage_route",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO etl.stage_segment_hit (
        provider_key
        ,[date_key]
        ,[segment_key]
        ,[hash]
        ,[datetime]
        ,[vehicle_type]
        ,[propulsion_type]
        ,[heading]
        ,[speed]
        ,[seen]
        ,[batch]
    )
    SELECT p.[key]
        ,[date_key]
        ,[segment_key]
        ,[hash]
        ,[datetime]
        ,[vehicle_type]
        ,[propulsion_type]
        ,[heading]
        ,[speed]
        ,[seen]
        ,[batch]
    FROM [etl].[extract_segment_hit] AS e
    LEFT JOIN dim.provider AS p ON p.[provider_id] = e.[provider_id]
    LEFT JOIN dim.vehicle as v ON (
        v.vehicle_id = source.vehicle_id
        AND v.device_id = source.device_id
    )
    WHERE batch = '{{ ts_nodash }}'
    """
)
clean_stage_task >> route_stage_task
provider_sync_task >> route_stage_task
vehicle_sync_task >> route_stage_task


route_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_route",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    UPDATE fact.segment_hit
    SET last_seen = source.seen
    FROM etl.stage_segment_hit AS source
    WHERE source.hash = fact.segment_hit.hash
    AND source.batch = '{{ ts_nodash }}'
    """
)

route_stage_task >> route_warehouse_update_task

route_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_route",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO fact.segment_hit
    (
        provider_key
        ,[date_key]
        ,[segment_key]
        ,[hash]
        ,[datetime]
        ,[vehicle_type]
        ,[propulsion_type]
        ,[heading]
        ,[speed]
        ,[first_seen]
        ,[last_seen]
    )
    SELECT p.[key]
        ,[date_key]
        ,[segment_key]
        ,[hash]
        ,[datetime]
        ,[vehicle_type]
        ,[propulsion_type]
        ,[heading]
        ,[speed]
        ,[seen]
        ,[seen]
    FROM [etl].[extract_segment_hit] AS e
    LEFT JOIN dim.provider AS p ON p.[provider_id] = e.[provider_id]
    WHERE batch = '{{ ts_nodash }}'
    AND NOT EXISTS (
        SELECT 1
        FROM fact.segment_hit AS AS target
        WHERE target.hash = source.hash
    )
    """
)

route_stage_task >> route_warehouse_insert_task

