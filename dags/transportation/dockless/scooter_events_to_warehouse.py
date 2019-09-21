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

providers = ["lime", "spin", "bolt", "shared", "razor", "bird"]

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

    events_remote_path = f"/transportation/mobility/etl/event/{provider}-{{{{ ts_nodash }}}}.csv"

    event_extract_task = MobilityEventsToSqlExtractOperator(
        task_id=f"loading_{provider}_events",
        provide_context=True,
        mobility_provider_conn_id=mobility_provider_conn_id,
        mobility_provider_token_conn_id=mobility_provider_token_conn_id,
        sql_conn_id="azure_sql_server_default",
        data_lake_conn_id="azure_data_lake_default",
        events_local_path=f"/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-{{{{ ts_nodash }}}}.csv",
        events_remote_path=events_remote_path,
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
    insert into
        etl.extract_event (
            [event_hash],
            [provider_id],
            [provider_name],
            [device_id],
            [vehicle_id],
            [vehicle_type],
            [propulsion_type],
            [date_key],
            [event_time],
            [state],
            [event],
            [cell_key],
            [census_block_group_key],
            [city_key],
            [county_key],
            [neighborhood_key],
            [park_key],
            [parking_district_key],
            [pattern_area_key],
            [zipcode_key],
            [battery_pct],
            [associated_trip],
            [seen],
            [batch]
        )
    select
        [event_hash],
        [provider_id],
        [provider_name],
        [device_id],
        [vehicle_id],
        [vehicle_type],
        [propulsion_type],
        [date_key],
        [event_time],
        [state],
        [event],
        [cell_key],
        [census_block_group_key],
        [city_key],
        [county_key],
        [neighborhood_key],
        [park_key],
        [parking_district_key],
        [pattern_area_key],
        [zipcode_key],
        [battery_pct],
        [associated_trip],
        [seen],
        [batch]
    from
        etl.external_event
    where
        batch = '{{ ts_nodash }}'
    """
)

task2 >> event_external_stage_task

# Run SQL scripts to transform extract data into staged facts
event_stage_task = MsSqlOperator(
    task_id=f"staging_states",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    insert into
        etl.stage_state (
            provider_key,
            vehicle_key,
            propulsion_type,
            start_hash,
            start_date_key,
            start_state,
            start_event,
            start_time,
            start_cell_key,
            start_census_block_group_key,
            start_city_key,
            start_county_key,
            start_neighborhood_key,
            start_park_key,
            start_parking_district_key,
            start_pattern_area_key,
            start_zipcode_key,
            start_battery_pct,
            end_hash,
            end_date_key,
            end_state,
            end_event,
            end_time,
            end_cell_key,
            end_census_block_group_key,
            end_city_key,
            end_county_key,
            end_neighborhood_key,
            end_park_key,
            end_parking_district_key,
            end_pattern_area_key,
            end_zipcode_key,
            end_battery_pct,
            associated_trip,
            duration,
            seen,
            batch
        )
    select
        p.[key],
        v.[key],
        propulsion_type,
        event_hash,
        date_key,
        state,
        event,
        event_time,
        cell_key,
        census_block_group_key,
        city_key,
        county_key,
        neighborhood_key,
        park_key,
        parking_district_key,
        pattern_area_key,
        zipcode_key,
        battery_pct,
        lead(event_hash) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(date_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(state) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(event) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(event_time) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(cell_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(census_block_group_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(city_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(county_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(neighborhood_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(park_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(parking_district_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(pattern_area_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(zipcode_key) over(partition by e.device_id, e.vehicle_id order by event_time),
        lead(battery_pct) over(partition by e.device_id, e.vehicle_id order by event_time),
        coalesce(associated_trip, lead(associated_trip) over(partition by e.device_id, e.vehicle_id order by event_time)),
        datediff(second, event_time, lead(event_time) over(partition by e.device_id, e.vehicle_id order by event_time)),
        seen,
        batch
    from
        etl.extract_event as e
    left join
        dim.provider as p on p.provider_id = e.provider_id
    left join
        dim.vehicle as v on (
            v.device_id = e.device_id
            and v.vehicle_id = e.vehicle_id
        )
    where
        e.batch = '{{ ts_nodash }}'
        """
)

clean_extract_table_task = MsSqlOperator(
    task_id="clean_extract_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    delete
    from
        etl.extract_event
    where
        batch = '{{ ts_nodash }}'
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
    delete
    from
        etl.stage_state
    where
        batch = '{{ ts_nodash }}'
    """
)

clean_stage_task_after = MsSqlOperator(
    task_id="clean_stage_table_after",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    delete
    from
        etl.stage_state
    where
        batch = '{{ ts_nodash }}'
    """
)
event_external_stage_task >> clean_stage_task_before >> event_stage_task


state_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_state",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    update
        fact.state
    set
        last_seen = source.seen,
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
    from
        etl.stage_state as source
    where
        source.start_hash = fact.state.start_hash
    and source.batch = '{{ ts_nodash }}'
    """
)

event_stage_task >> state_warehouse_update_task >> clean_stage_task_after

state_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_state",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    insert into
        [fact].[state] (
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
    select
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
    from
        [etl].[stage_state] as source
    where
        batch = '{{ ts_nodash }}'
    and not exists (
        select
            1
        from
            fact.state as target
        where
            target.start_hash = source.start_hash
    )
    """
)

event_stage_task >> state_warehouse_insert_task >> clean_stage_task_after

for task in remote_paths_delete_tasks:
    event_stage_task >> task
