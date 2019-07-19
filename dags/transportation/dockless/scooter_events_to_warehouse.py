"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator
from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityEventsToSqlExtractOperator,
    MobilityProviderSyncOperator,
    MobilityVehicleSyncOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date":  datetime(2019, 4, 26),
    "email": ["pbotsqldbas@portlandoregon.gov"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 9,
    "retry_delay": timedelta(minutes=1),
    "concurrency": 1,
    "max_active_runs": 1
    # "queue": "bash_queue",
    # "pool": "backfill",
    # "priority_weight": 10,
    # "end_date": datetime(2016, 1, 1),
}

dag = DAG(
    dag_id="scooter_events_to_warehouse",
    default_args=default_args,
    catchup=True,
    schedule_interval="@hourly",
)

providers = ["lime", "spin", "bolt", "shared", "razor"]

task1 = DummyOperator(
    task_id="provider_extract_start",
    dag=dag
)

task2 = DummyOperator(
    task_id="provider_extract_complete",
    dag=dag
)

remote_paths_delete_tasks = []

# Extract data from providers and stage in tables
for provider in providers:
    mobility_provider_conn_id = f"mobility_provider_{provider}"
    mobility_provider_token_conn_id = f"mobility_provider_{provider}_token"

    events_remote_path = f"/transportation/mobility/etl/event/{provider}-{{ ts_nodash }}.csv"

    event_extract_task = MobilityEventsToSqlExtractOperator(
        task_id=f"loading_{provider}_events",
        provide_context=True,
        mobility_provider_conn_id=mobility_provider_conn_id,
        mobility_provider_token_conn_id=mobility_provider_token_conn_id,
        sql_conn_id="azure_sql_server_default",
        data_lake_conn_id="azure_data_lake_default",
        events_local_path="/usr/local/airlow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{provider}-{{ ts_nodash }}.csv",
        events_remote_path=events_remote_path,
        cities_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/cities-{{ ts_nodash }}.csv",
        cities_remote_path="/transportation/mobility/dim/cities.csv",
        parking_districts_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parking_districts-{{ ts_nodash }}.csv",
        parking_districts_remote_path="/transportation/mobility/dim/parking_districts.csv",
        pattern_areas_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/pattern_areas-{{ ts_nodash }}.csv",
        pattern_areas_remote_path="/transportation/mobility/dim/pattern_areas.csv",

        #New Geometry
        census_blocks_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/census_blocks-{{ ts_nodash }}.csv",
        census_blocks_remote_path='/transportation/mobility/dim/census_blocks.csv',
        counties_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/counties-{{ ts_nodash }}.csv",
        counties_remote_path='/transportation/mobility/dim/counties.csv',
        neighborhoods_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/neighborhoods-{{ ts_nodash }}.csv",
        neighborhoods_remote_path='/transportation/mobility/dim/neighborhoods.csv',
        parks_local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parks-{{ ts_nodash }}.csv',
        parks_remote_path='/transportation/mobility/dim/parks.csv',
        zipcodes_local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/zipcodes-{{ ts_nodash }}.csv',
        zipcodes_remote_path='/transportation/mobility/dim/zipcodes.csv',
        dag=dag)

    remote_paths_delete_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f"delete_{provider}_extract",
                                    dag=dag,
                                    azure_data_lake_conn_id="azure_data_lake_default",
                                    remote_path=events_remote_path))

    event_extract_task.set_upstream(task1)
    event_extract_task.set_downstream(task2)

event_external_stage_task = MsSqlOperator(
    task_id="extract_external_batch",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO etl.extract_event (
        [event_hash]
        ,[provider_id]
        ,[provider_name]
        ,[device_id]
        ,[vehicle_id]
        ,[vehicle_type]
        ,[propulsion_type]
        ,[date_key]
        ,[event_time]
        ,[state]
        ,[event]
        ,[cell_key]
        ,[census_block_group_key]
        ,[city_key]
        ,[county_key]
        ,[neighborhood_key]
        ,[park_key]
        ,[parking_district_key]
        ,[pattern_area_key]
        ,[zipcode_key]
        ,[battery_pct]
        ,[associated_trip]
        ,[seen]
        ,[batch]
    )
    SELECT
    [event_hash]
    ,[provider_id]
    ,[provider_name]
    ,[device_id]
    ,[vehicle_id]
    ,[vehicle_type]
    ,[propulsion_type]
    ,[date_key]
    ,[event_time]
    ,[state]
    ,[event]
    ,[cell_key]
    ,[census_block_group_key]
    ,[city_key]
    ,[county_key]
    ,[neighborhood_key]
    ,[park_key]
    ,[parking_district_key]
    ,[pattern_area_key]
    ,[zipcode_key]
    ,[battery_pct]
    ,[associated_trip]
    ,[seen]
    ,[batch]
    FROM etl.external_event
    WHERE batch = '{{ ts_nodash }}'
    """
)

task2 >> event_external_stage_task

# Run SQL scripts to transform extract data into staged facts
event_stage_task = MsSqlOperator(
    task_id=f"staging_states",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
        INSERT INTO etl.stage_state (
            provider_key
            ,vehicle_key
            ,propulsion_type
            ,start_hash
            ,start_date_key
            ,start_state
            ,start_event
            ,start_time
            ,start_cell_key
            ,start_census_block_group_key
            ,start_city_key
            ,start_county_key
            ,start_neighborhood_key
            ,start_park_key
            ,start_parking_district_key
            ,start_pattern_area_key
            ,start_zipcode_key
            ,start_battery_pct
            ,end_hash
            ,end_date_key
            ,end_state
            ,end_event
            ,end_time
            ,end_cell_key
            ,end_census_block_group_key
            ,end_city_key
            ,end_county_key
            ,end_neighborhood_key
            ,end_park_key
            ,end_parking_district_key
            ,end_pattern_area_key
            ,end_zipcode_key
            ,end_battery_pct
            ,associated_trip
            ,duration
            ,seen
            ,batch
        )
        SELECT
        p.[key]
        ,v.[key]
        ,propulsion_type
        ,event_hash
        ,date_key
        ,state
        ,event
        ,event_time
        ,cell_key
        ,census_block_group_key
        ,city_key
        ,county_key
        ,neighborhood_key
        ,park_key
        ,parking_district_key
        ,pattern_area_key
        ,zipcode_key
        ,battery_pct
        ,LEAD(event_hash) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(date_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(state) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(event) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(event_time) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(cell_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(census_block_group_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(city_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(county_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(neighborhood_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(park_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(parking_district_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(pattern_area_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(zipcode_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(battery_pct) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,COALESCE(associated_trip, LEAD(associated_trip) OVER(PARTITION BY e.device_id ORDER BY event_time))
        ,DATEDIFF(SECOND, event_time, LEAD(event_time) OVER(PARTITION BY e.device_id ORDER BY event_time))
        ,seen
        ,batch
        FROM etl.extract_event AS e
        LEFT JOIN dim.provider AS p ON p.provider_id = e.provider_id
        LEFT JOIN dim.vehicle AS v ON v.device_id = e.device_id
        WHERE e.batch = '{{ ts_nodash }}'
        """
)

clean_extract_table_task = MsSqlOperator(
    task_id="clean_extract_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.extract_event WHERE batch = '{{ ts_nodash }}'
    """
)

event_stage_task >> clean_extract_table_task

provider_sync_task = MobilityProviderSyncOperator(
    task_id="provider_sync",
    source_table="etl.extract_event",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)
provider_sync_task.set_upstream(event_external_stage_task)
provider_sync_task.set_downstream(event_stage_task)

vehicle_sync_task = MobilityVehicleSyncOperator(
    task_id="vehicle_sync",
    source_table="etl.extract_event",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)
vehicle_sync_task.set_upstream(event_external_stage_task)
vehicle_sync_task.set_downstream(event_stage_task)

clean_stage_task_before = MsSqlOperator(
    task_id="clean_stage_table_before",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_state WHERE batch = '{{ ts_nodash }}'
    """
)

clean_stage_task_after = MsSqlOperator(
    task_id="clean_stage_table_after",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_state WHERE batch = '{{ ts_nodash }}'
    """
)
event_external_stage_task >> clean_stage_task_before >> event_stage_task


state_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_state",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    UPDATE fact.state
    SET last_seen = source.seen,
    [start_cell_key] = source.start_cell_key,
    [start_census_block_group_key] = source.start_census_block_group_key,
    [start_city_key] = source.start_city_key,
    [start_county_key] = source.start_county_key,
    [start_neighborhood_key] = source.start_neighborhood_key,
    [start_park_key] = source.start_park_key,
    [start_parking_district_key] = source.start_parking_district_key,
    [start_pattern_area_key] = source.start_pattern_area_key,
    [start_zipcode_key] = source.start_zipcode_key,
    [end_cell_key] = source.end_cell_key,
    [end_census_block_group_key] = source.end_census_block_group_key,
    [end_city_key] = source.end_city_key,
    [end_county_key] = source.end_county_key,
    [end_neighborhood_key] = source.end_neighborhood_key,
    [end_park_key] = source.end_park_key,
    [end_parking_district_key] = source.end_parking_district_key,
    [end_pattern_area_key] = source.end_pattern_area_key,
    [end_zipcode_key] = source.end_zipcode_key
    FROM etl.stage_state AS source
    WHERE source.start_hash = fact.state.start_hash
    AND source.batch = '{{ ts_nodash }}'
    """
)

event_stage_task >> state_warehouse_update_task >> clean_stage_task_after

state_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_state",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO [fact].[state] (
        [provider_key],
        [vehicle_key],
        [propulsion_type],
        [start_hash],
        [start_date_key],
        [start_time],
        [start_state],
        [start_event],
        [start_cell_key],
        [start_census_block_group_key],
        [start_city_key],
        [start_county_key],
        [start_neighborhood_key],
        [start_park_key],
        [start_parking_district_key],
        [start_pattern_area_key],
        [start_zipcode_key],
        [start_battery_pct],
        [end_hash],
        [end_date_key],
        [end_time],
        [end_state],
        [end_event],
        [end_cell_key],
        [end_census_block_group_key],
        [end_city_key],
        [end_county_key],
        [end_neighborhood_key],
        [end_park_key],
        [end_parking_district_key],
        [end_pattern_area_key],
        [end_zipcode_key],
        [end_battery_pct],
        [associated_trip],
        [duration],
        [first_seen],
        [last_seen]
    )
    SELECT
    [provider_key],
    [vehicle_key],
    [propulsion_type],
    [start_hash],
    [start_date_key],
    [start_time],
    [start_state],
    [start_event],
    [start_cell_key],
    [start_census_block_group_key],
    [start_city_key],
    [start_county_key],
    [start_neighborhood_key],
    [start_park_key],
    [start_parking_district_key],
    [start_pattern_area_key],
    [start_zipcode_key],
    [start_battery_pct],
    [end_hash],
    [end_date_key],
    [end_time],
    [end_state],
    [end_event],
    [end_cell_key],
    [end_census_block_group_key],
    [end_city_key],
    [end_county_key],
    [end_neighborhood_key],
    [end_park_key],
    [end_parking_district_key],
    [end_pattern_area_key],
    [end_zipcode_key],
    [end_battery_pct],
    [associated_trip],
    [duration],
    [seen],
    [seen]
    FROM [etl].[stage_state] AS source
    WHERE batch = '{{ ts_nodash }}'
    AND NOT EXISTS (
        SELECT 1
        FROM fact.state AS target
        WHERE target.start_hash = source.start_hash
    )
    """
)

event_stage_task >> state_warehouse_insert_task >> clean_stage_task_after

for task in remote_paths_delete_tasks:
    event_stage_task >> task
