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
    MobilityTripsToSqlExtractOperator,
    MobilityProviderSyncOperator,
    MobilityVehicleSyncOperator
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date":  datetime(2019, 4, 26),
    "email": ["pbotsqldbas@portlandoregon.gov"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 9,
    "retry_delay": timedelta(minutes=1),
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

providers = ["lime", "spin", "bolt", "shared", "razor", "bird"]

task1 = DummyOperator(
    task_id="provider_extract_start",
    dag=dag
)

task2 = DummyOperator(
    task_id="provider_extract_complete",
    dag=dag
)

trip_remote_files_delete_tasks = []
segment_hits_remote_files_delete_tasks = []

# Extract data from providers and stage in tables
for provider in providers:
    mobility_provider_conn_id = f"mobility_provider_{provider}"
    mobility_provider_token_conn_id = f"mobility_provider_{provider}_token"

    trips_remote_path = f"/transportation/mobility/etl/trip/{provider}-{{{{ ts_nodash }}}}.csv"
    segment_hits_remote_path = f"/transportation/mobility/etl/segment_hit/{provider}-{{{{ ts_nodash }}}}.csv"

    trip_extract_task = MobilityTripsToSqlExtractOperator(
        task_id=f"loading_{provider}_trips",
        provide_context=True,
        mobility_provider_conn_id=mobility_provider_conn_id,
        mobility_provider_token_conn_id=mobility_provider_token_conn_id,
        sql_conn_id="azure_sql_server_default",
        data_lake_conn_id="azure_data_lake_default",
        trips_local_path=f"/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-trips-{{{{ ts_nodash }}}}.csv",
        trips_remote_path=trips_remote_path,
        segment_hits_local_path=f"/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-segment-hits-{{{{ ts_nodash }}}}.csv",
        segment_hits_remote_path=segment_hits_remote_path,
        cities_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/cities-{{ ts_nodash }}.csv",
        cities_remote_path="/transportation/mobility/dim/cities.csv",
        parking_districts_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parking_districts-{{ ts_nodash }}.csv",
        parking_districts_remote_path="/transportation/mobility/dim/parking_districts.csv",
        pattern_areas_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/pattern_areas-{{ ts_nodash }}.csv",
        pattern_areas_remote_path="/transportation/mobility/dim/pattern_areas.csv",
        census_blocks_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/census_block_groups-{{ ts_nodash }}.csv",
        census_blocks_remote_path='/transportation/mobility/dim/census_block_groups.csv',
        counties_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/counties-{{ ts_nodash }}.csv",
        counties_remote_path='/transportation/mobility/dim/counties.csv',
        neighborhoods_local_path="/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/neighborhoods-{{ ts_nodash }}.csv",
        neighborhoods_remote_path='/transportation/mobility/dim/neighborhoods.csv',
        parks_local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parks-{{ ts_nodash }}.csv',
        parks_remote_path='/transportation/mobility/dim/parks.csv',
        zipcodes_local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/zipcodes-{{ ts_nodash }}.csv',
        zipcodes_remote_path='/transportation/mobility/dim/zipcodes.csv',
        dag=dag)

    trip_remote_files_delete_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f"delete_{provider}_trip_extract",
                                    dag=dag,
                                    azure_data_lake_conn_id="azure_data_lake_default",
                                    remote_path=trips_remote_path))

    segment_hits_remote_files_delete_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f"delete_{provider}_route_extract",
                                    dag=dag,
                                    azure_data_lake_conn_id="azure_data_lake_default",
                                    remote_path=segment_hits_remote_path))

    trip_extract_task.set_upstream(task1)
    trip_extract_task.set_downstream(task2)

trip_external_stage_task = MsSqlOperator(
    task_id="extract_external_trips",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO etl.extract_trip (
        [trip_id]
        ,[provider_id]
        ,[provider_name]
        ,[device_id]
        ,[vehicle_id]
        ,[vehicle_type]
        ,[propulsion_type]
        ,[start_time]
        ,[start_date_key]
        ,[start_cell_key]
        ,[start_census_block_group_key]
        ,[start_city_key]
        ,[start_county_key]
        ,[start_neighborhood_key]
        ,[start_park_key]
        ,[start_parking_district_key]
        ,[start_pattern_area_key]
        ,[start_zipcode_key]
        ,[end_time]
        ,[end_date_key]
        ,[end_cell_key]
        ,[end_census_block_group_key]
        ,[end_city_key]
        ,[end_county_key]
        ,[end_neighborhood_key]
        ,[end_park_key]
        ,[end_parking_district_key]
        ,[end_pattern_area_key]
        ,[end_zipcode_key]
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
    [trip_id]
    ,[provider_id]
    ,[provider_name]
    ,[device_id]
    ,[vehicle_id]
    ,[vehicle_type]
    ,[propulsion_type]
    ,[start_time]
    ,[start_date_key]
    ,[start_cell_key]
    ,[start_census_block_group_key]
    ,[start_city_key]
    ,[start_county_key]
    ,[start_neighborhood_key]
    ,[start_park_key]
    ,[start_parking_district_key]
    ,[start_pattern_area_key]
    ,[start_zipcode_key]
    ,[end_time]
    ,[end_date_key]
    ,[end_cell_key]
    ,[end_census_block_group_key]
    ,[end_city_key]
    ,[end_county_key]
    ,[end_neighborhood_key]
    ,[end_park_key]
    ,[end_parking_district_key]
    ,[end_pattern_area_key]
    ,[end_zipcode_key]
    ,[distance]
    ,[duration]
    ,[accuracy]
    ,[standard_cost]
    ,[actual_cost]
    ,[parking_verification_url]
    ,[seen]
    ,[batch]
    FROM etl.external_trip
    WHERE batch = '{{ ts_nodash }}'
    """
)

segment_hit_external_stage_task = MsSqlOperator(
    task_id="extract_external_segment_hits",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO etl.extract_segment_hit (
        [provider_id]
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
    SELECT
    [provider_id]
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
    FROM etl.external_segment_hit
    WHERE batch = '{{ ts_nodash }}'
    """
)

task2 >> trip_external_stage_task
task2 >> segment_hit_external_stage_task

clean_stage_task_before = MsSqlOperator(
    task_id="clean_stage_table_before",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_trip WHERE batch = '{{ ts_nodash }}'
    DELETE FROM etl.stage_segment_hit WHERE batch = '{{ ts_nodash }}'
    """
)

clean_stage_task_after = MsSqlOperator(
    task_id="clean_stage_table_after",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_trip WHERE batch = '{{ ts_nodash }}'
    DELETE FROM etl.stage_segment_hit WHERE batch = '{{ ts_nodash }}'
    """
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

trip_external_stage_task >> clean_stage_task_before
segment_hit_external_stage_task >> clean_stage_task_before

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
        ,[start_cell_key]
        ,[start_census_block_group_key]
        ,[start_city_key]
        ,[start_county_key]
        ,[start_neighborhood_key]
        ,[start_park_key]
        ,[start_parking_district_key]
        ,[start_pattern_area_key]
        ,[start_zipcode_key]
        ,[end_cell_key]
        ,[end_census_block_group_key]
        ,[end_city_key]
        ,[end_county_key]
        ,[end_neighborhood_key]
        ,[end_park_key]
        ,[end_parking_district_key]
        ,[end_pattern_area_key]
        ,[end_zipcode_key]
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
    ,[start_cell_key]
    ,[start_census_block_group_key]
    ,[start_city_key]
    ,[start_county_key]
    ,[start_neighborhood_key]
    ,[start_park_key]
    ,[start_parking_district_key]
    ,[start_pattern_area_key]
    ,[start_zipcode_key]
    ,[end_cell_key]
    ,[end_census_block_group_key]
    ,[end_city_key]
    ,[end_county_key]
    ,[end_neighborhood_key]
    ,[end_park_key]
    ,[end_parking_district_key]
    ,[end_pattern_area_key]
    ,[end_zipcode_key]
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
    LEFT JOIN dim.vehicle AS v ON (
        v.vehicle_id = source.vehicle_id
        AND v.device_id = source.device_id
    )
    WHERE batch = '{{ ts_nodash }}'
    """
)

clean_stage_task_before >> trip_stage_task

trip_stage_task >> clean_extract_task

trip_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_trip",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    UPDATE fact.trip
    SET last_seen = source.seen,
    start_cell_key = source.start_cell_key,
    start_census_block_group_key = source.start_census_block_group_key,
    start_city_key = source.start_city_key,
    start_county_key = source.start_county_key,
    start_neighborhood_key = source.start_neighborhood_key,
    start_park_key = source.start_park_key,
    start_parking_district_key = source.start_parking_district_key,
    start_pattern_area_key = source.start_pattern_area_key,
    start_zipcode_key = source.start_zipcode_key,
    end_cell_key = source.end_cell_key,
    end_census_block_group_key = source.end_census_block_group_key,
    end_city_key = source.end_city_key,
    end_county_key = source.end_county_key,
    end_neighborhood_key = source.end_neighborhood_key,
    end_park_key = source.end_park_key,
    end_parking_district_key = source.end_parking_district_key,
    end_pattern_area_key = source.end_pattern_area_key,
    end_zipcode_key = source.end_zipcode_key
    FROM etl.stage_trip AS source
    WHERE source.trip_id = fact.trip.trip_id
    AND source.batch = '{{ ts_nodash }}'
    """
)

trip_stage_task >> trip_warehouse_update_task >> clean_stage_task_after

trip_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_trip",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    INSERT INTO [fact].[trip] (
        [trip_id],
        [provider_key],
        [vehicle_key],
        [propulsion_type],
        [start_time],
        [start_date_key],
        [start_cell_key],
        [start_census_block_group_key],
        [start_city_key],
        [start_county_key],
        [start_neighborhood_key],
        [start_park_key],
        [start_parking_district_key],
        [start_pattern_area_key],
        [start_zipcode_key],
        [end_time],
        [end_date_key],
        [end_cell_key],
        [end_census_block_group_key],
        [end_city_key],
        [end_county_key],
        [end_neighborhood_key],
        [end_park_key],
        [end_parking_district_key],
        [end_pattern_area_key],
        [end_zipcode_key],
        [distance],
        [duration],
        [accuracy],
        [standard_cost],
        [actual_cost],
        [parking_verification_url],
        [first_seen],
        [last_seen]
    )
    SELECT
    [trip_id],
    [provider_key],
    [vehicle_key],
    [propulsion_type],
    [start_time],
    [start_date_key],
    [start_cell_key],
    [start_census_block_group_key],
    [start_city_key],
    [start_county_key],
    [start_neighborhood_key],
    [start_park_key],
    [start_parking_district_key],
    [start_pattern_area_key],
    [start_zipcode_key],
    [end_time],
    [end_date_key],
    [end_cell_key],
    [end_census_block_group_key],
    [end_city_key],
    [end_county_key],
    [end_neighborhood_key],
    [end_park_key],
    [end_parking_district_key],
    [end_pattern_area_key],
    [end_zipcode_key],
    [distance],
    [duration],
    [accuracy],
    [standard_cost],
    [actual_cost],
    [parking_verification_url],
    [seen],
    [seen]
    FROM [etl].[stage_trip] AS source
    WHERE batch = '{{ ts_nodash }}'
    AND NOT EXISTS (
        SELECT 1
        FROM fact.trip AS target
        WHERE target.trip_id = source.trip_id
    )
    """
)

trip_stage_task >> trip_warehouse_insert_task >> clean_stage_task_after

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
    WHERE batch = '{{ ts_nodash }}'
    """
)
clean_stage_task_before >> route_stage_task

route_stage_task >> clean_extract_task

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

route_stage_task >> route_warehouse_update_task >> clean_stage_task_after

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
    SELECT [provider_key]
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
    FROM [etl].[stage_segment_hit] AS source
    WHERE batch = '{{ ts_nodash }}'
    AND NOT EXISTS (
        SELECT 1
        FROM fact.segment_hit AS target
        WHERE target.hash = source.hash
    )
    """
)

route_stage_task >> route_warehouse_insert_task >> clean_stage_task_after


for task in trip_remote_files_delete_tasks:
    trip_warehouse_insert_task >> task
    trip_warehouse_update_task >> task
for task in segment_hits_remote_files_delete_tasks:
    route_warehouse_insert_task >> task
    route_warehouse_update_task >> task
