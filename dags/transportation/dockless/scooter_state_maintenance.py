"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from datetime import datetime, timedelta

import airflow
from airflow import DAG

from airflow.operators.mssql_plugin import MsSqlOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date":  datetime(2019, 5, 1),
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
    dag_id="scooter_state_maintenance",
    default_args=default_args,
    catchup=True,
    schedule_interval="@daily"
)

extract_null_states_task = MsSqlOperator(
    task_id="extract_null_end_states",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    insert into
        etl.extract_maintenance_states
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
        [associated_trip],
        [duration],
        getdate(),
        '{{ ts_nodash }}'
    from
        fact.state as source
    where
        end_hash is null
    """
)

extract_lost_states_task = MsSqlOperator(
    task_id="extract_lost_end_states",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    insert into
        etl.extract_maintenance_states (
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
            [associated_trip],
            [duration],
            [seen],
            [batch]
        )
    select
        [provider_key],
        [vehicle_key],
        [propulsion_type],
        'unknown',
        convert(int, convert(varchar(30), dateadd(hour, 48, start_time), 112)),
        dateadd(hour, 48, start_time),
        'unknown',
        'unknown',
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
        [associated_trip],
        null,
        getdate(),
        '{{ ts_nodash }}'
    from
        fact.state as source
    where
        end_hash is null
        and start_state not in ('unknown', 'removed')
        and (getdate() at time zone 'Pacific Standard Time') > dateadd(hour, 48, start_time)
    """
)

stage_states_task = MsSqlOperator(
    task_id="stage_state_maintenance",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    insert into
        etl.stage_maintenance_states (
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
            [batch]
        )
    select
        s1.[provider_key],
        s1.[vehicle_key],
        s1.[propulsion_type],
        s1.[start_hash],
        s1.[start_date_key],
        s1.[start_time],
        s1.[start_state],
        s1.[start_event],
        s1.[start_cell_key],
        s1.[start_census_block_group_key],
        s1.[start_city_key],
        s1.[start_county_key],
        s1.[start_neighborhood_key],
        s1.[start_park_key],
        s1.[start_parking_district_key],
        s1.[start_pattern_area_key],
        s1.[start_zipcode_key],
        s1.[start_battery_pct],
        [next_state].[start_hash],
        [next_state].[start_date_key],
        [next_state].[start_time],
        [next_state].[start_state],
        [next_state].[start_event],
        [next_state].[start_cell_key],
        [next_state].[start_census_block_group_key],
        [next_state].[start_city_key],
        [next_state].[start_county_key],
        [next_state].[start_neighborhood_key],
        [next_state].[start_park_key],
        [next_state].[start_parking_district_key],
        [next_state].[start_pattern_area_key],
        [next_state].[start_zipcode_key],
        [next_state].[start_battery_pct],
        coalesce(
            s1.[associated_trip],
            [next_state].[associated_trip]
        ),
        datediff(
            second,
            s1.[start_time],
            [next_state].[start_time]
        ),
        s1.[seen],
        s1.[batch]
    from
        etl.extract_maintenance_states as s1
        outer apply (
            select
                top 1 start_hash,
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
                associated_trip
            from
                fact.state as s2
            where
                s2.vehicle_key = s1.vehicle_key
                and s2.start_time > s1.start_time
            order by
                s2.start_time
        ) as next_state
    where
        batch = '{{ ts_nodash }}'
    """
)

update_null_end_states_task = MsSqlOperator(
    task_id="update_null_end_states",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    update
        etl.stage_maintenance_states
    set
        end_hash = 'unknown',
        end_date_key = convert(int, convert(varchar(30), dateadd(hour, 48, start_time), 112)),
        end_state = 'unknown',
        end_event = 'unknown',
        end_time = dateadd(hour, 48, start_time),
        end_cell_key = start_cell_key,
        end_census_block_group_key = start_census_block_group_key,
        end_city_key = start_city_key,
        end_county_key = start_county_key,
        end_neighborhood_key = start_neighborhood_key,
        end_park_key = start_park_key,
        end_parking_district_key = start_parking_district_key,
        end_pattern_area_key = start_pattern_area_key,
        end_zipcode_key = start_zipcode_key,
        end_battery_pct = start_battery_pct,
        associated_trip = associated_trip,
        duration = null,
        last_seen = getdate()
    where
        batch = '{{ ts_nodash }}'
        and end_hash is null
        and start_state not in ('unknown', 'removed')
        and (getdate() at time zone 'Pacific Standard Time') > dateadd(hour, 48, start_time)
    """
)

warehouse_update_task = MsSqlOperator(
    task_id="update_warehouse_states",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    update
        fact.state
    set
        end_hash = source.end_hash,
        end_date_key = source.end_date_key,
        end_state = source.end_state,
        end_event = source.end_event,
        end_time = source.end_time,
        end_cell_key = source.end_cell_key,
        end_census_block_group_key = source.end_census_block_group_key,
        end_city_key = source.end_city_key,
        end_county_key = source.end_county_key,
        end_neighborhood_key = source.end_neighborhood_key,
        end_park_key = source.end_park_key,
        end_parking_district_key = source.end_parking_district_key,
        end_pattern_area_key = source.end_pattern_area_key,
        end_zipcode_key = source.end_zipcode_key,
        end_battery_pct = source.end_battery_pct,
        associated_trip = source.associated_trip,
        duration = source.duration,
        last_seen = source.seen
    from
        etl.stage_maintenance_states as source
    where
        source.batch = '{{ ts_nodash }}'
        and source.start_hash = fact.state.start_hash
    """
)

warehouse_insert_unknown_task = MsSqlOperator(
    task_id="insert_warehouse_unknown_state",
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
        [associated_trip],
        [duration],
        [seen],
        [seen]
    from
        etl.stage_maintenance_states as source
    where
        batch = '{{ ts_nodash }}'
        and start_state = 'unknown'
        and not exists (
            select
                1
            from
                fact.state as target
            where
                target.vehicle_key = source.vehicle_key
                and target.end_hash is null
                and target.start_state = 'unknown'
        )
    """
)

extract_null_states_task >> stage_states_task << extract_lost_states_task
stage_states_task >> update_null_end_states_task
warehouse_update_task << update_null_end_states_task >> warehouse_insert_unknown_task
