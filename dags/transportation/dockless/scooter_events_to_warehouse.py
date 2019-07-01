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
    "retry_delay": timedelta(minutes=2),
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
        cities_local_path=f"/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/cities-{{{{ ts_nodash }}}}.csv",
        cities_remote_path=f"/transportation/mobility/dim/cities.csv",
        parking_districts_local_path=f"/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/parking_districts-{{{{ ts_nodash }}}}.csv",
        parking_districts_remote_path=f"/transportation/mobility/dim/parking_districts.csv",
        pattern_areas_local_path=f"/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/pattern_areas-{{{{ ts_nodash }}}}.csv",
        pattern_areas_remote_path=f"/transportation/mobility/dim/pattern_areas.csv",
        dag=dag)

    remote_paths_delete_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f"delete_{provider}_extract",
                                    dag=dag,
                                    azure_data_lake_conn_id="azure_data_lake_default",
                                    remote_path=events_remote_path))

    event_extract_task.set_upstream(task1)
    event_extract_task.set_downstream(task2)

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
            ,start_city_key
            ,start_parking_district_key
            ,start_pattern_area_key
            ,start_battery_pct
            ,end_hash
            ,end_date_key
            ,end_state
            ,end_event
            ,end_time
            ,end_cell_key
            ,end_city_key
            ,end_parking_district_key
            ,end_pattern_area_key
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
        ,city_key
        ,parking_district_key
        ,pattern_area_key
        ,battery_pct
        ,LEAD(event_hash) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(date_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(state) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(event) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(event_time) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(cell_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(city_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(parking_district_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(pattern_area_key) OVER(PARTITION BY e.device_id ORDER BY event_time)
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

provider_sync_task = MobilityProviderSyncOperator(
    task_id="provider_sync",
    source_table="etl.extract_event",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)
provider_sync_task.set_upstream(task2)
provider_sync_task.set_downstream(event_stage_task)

vehicle_sync_task = MobilityVehicleSyncOperator(
    task_id="vehicle_sync",
    source_table="etl.extract_event",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)
vehicle_sync_task.set_upstream(task2)
vehicle_sync_task.set_downstream(event_stage_task)

clean_stage_task = MsSqlOperator(
    task_id="clean_stage_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_state WHERE batch = '{{ ts_nodash }}'
    """
)
clean_stage_task.set_upstream(task2)
clean_stage_task.set_downstream(event_stage_task)

task3 = DummyOperator(
    task_id="provider_staging_complete",
    dag=dag
)

for task in remote_paths_delete_tasks:
    event_stage_task >> task

state_warehouse_update_task = MsSqlOperator(
    task_id="warehouse_update_trip",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    UPDATE fact.state
    SET last_seen = source.seen,
    [start_cell_key] = source.start_cell_key,
    [start_city_key] = source.start_city_key,
    [start_parking_district_key] = source.start_parking_district_key,
    [start_pattern_area_key] = source.start_pattern_area_key,
    [end_cell_key] = source.end_cell_key,
    [end_city_key] = source.end_city_key,
    [end_parking_district_key] = source.end_parking_district_key,
    [end_pattern_area_key] = source.end_pattern_area_key
    FROM etl.stage_state AS source
    WHERE source.start_hash = fact.state.start_hash
    AND source.batch = '{{ ts_nodash }}'
    """
)

event_stage_task >> state_warehouse_update_task

state_warehouse_insert_task = MsSqlOperator(
    task_id="warehouse_insert_trip",
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
        [start_city_key],
        [start_parking_district_key],
        [start_pattern_area_key],
        [start_battery_pct],
        [end_hash],
        [end_date_key],
        [end_time],
        [end_state],
        [end_event],
        [end_cell_key],
        [end_city_key],
        [end_parking_district_key],
        [end_pattern_area_key],
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
    [start_city_key],
    [start_parking_district_key],
    [start_pattern_area_key],
    [start_battery_pct],
    [end_hash],
    [end_date_key],
    [end_time],
    [end_state],
    [end_event],
    [end_cell_key],
    [end_city_key],
    [end_parking_district_key],
    [end_pattern_area_key],
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

event_stage_task >> state_warehouse_insert_task
