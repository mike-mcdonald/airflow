from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.mobility_plugin import MobilityProviderHook
from airflow.hooks.dataframe_plugin import AzureMsSqlDataFrameHook

from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator
from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityProviderSyncOperator,
    MobilityVehicleSyncOperator,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2019, 4, 26),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
    'max_active_runs': 1
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='scooter_states_to_warehouse',
    default_args=default_args,
    catchup=True,
    schedule_interval='@daily',
)

EVENT_WEIGHT = {
    'service_start': 1,
    'user_drop_off': 2,
    'rebalance_drop_off': 3,
    'maintenance_drop_off': 4,
    'user_pick_up': 5,
    'maintenance': 6,
    'low_battery': 7,
    'service_end': 8,
    'rebalance_pick_up': 9,
    'maintenance_pick_up': 10
}

case_statement = f'''
case event
{
    ' '.join([f"when '{event}' then {weight}" for event, weight in EVENT_WEIGHT.items()])
}
end
'''

clean_extract_task = MsSqlOperator(
    task_id='clean_extract_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    delete
    from
        etl.extract_state
    where
        batch = '{{ ts_nodash }}'
    '''
)

extract_state_task = MsSqlOperator(
    task_id='extract_states',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql=f'''
    insert into
        etl.extract_state (
            provider_key,
            vehicle_key,
            date_key,
            hash,
            state,
            event,
            datetime,
            weight,
            propulsion_type,
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
            associated_trip,
            duration,
            seen,
            batch
        )
    select
        provider_key,
        vehicle_key,
        date_key,
        hash,
        state,
        event,
        datetime,
        {case_statement},
        propulsion_type,
        weight,
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
        associated_trip,
        getdate(),
        '{{{{ ts_nodash }}}}'
    from
        fact.event
    where
        datetime between '{{{{ execution_date.in_timezone('America/Los_Angeles').subtract(hours=96) }}}}' and '{{{{ execution_date.in_timezone('America/Los_Angeles') }}}}'
    '''
)

clean_extract_task >> extract_state_task

# Run SQL scripts to transform extract data into staged facts
stage_state_task = MsSqlOperator(
    task_id=f'stage_states',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
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
        provider_key,
        vehicle_key,
        propulsion_type,
        hash,
        date_key,
        state,
        event,
        datetime,
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
        lead(hash) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(date_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(state) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(event) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(datetime) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(cell_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(census_block_group_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(city_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(county_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(neighborhood_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(park_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(parking_district_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(pattern_area_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(zipcode_key) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        lead(battery_pct) over(partition by e.device_id, e.vehicle_id order by datetime, weight),
        coalesce(associated_trip, lead(associated_trip) over(partition by e.device_id, e.vehicle_id order by datetime, weight)),
        datediff(second, datetime, lead(datetime) over(partition by e.device_id, e.vehicle_id order by datetime, weight)),
        seen,
        '{{ ts_nodash }}'
    from
        etl.stage_state as e
    where
        e.batch = '{{ ts_nodash }}'
        '''
)

extract_state_task >> stage_state_task


state_warehouse_update_task = MsSqlOperator(
    task_id='warehouse_update_state',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    update
        fact.state
    set
        last_seen = source.seen,
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
    from
        etl.stage_state as source
    where
        source.start_hash = fact.state.start_hash
    and source.batch = '{{ ts_nodash }}'
    '''
)

state_warehouse_insert_task = MsSqlOperator(
    task_id='warehouse_insert_state',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    insert into
        fact.state (
            provider_key,
            vehicle_key,
            propulsion_type,
            start_hash,
            start_date_key,
            start_time,
            start_state,
            start_event,
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
            end_time,
            end_state,
            end_event,
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
            first_seen,
            last_seen
        )
    select
        provider_key,
        vehicle_key,
        propulsion_type,
        start_hash,
        start_date_key,
        start_time,
        start_state,
        start_event,
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
        end_time,
        end_state,
        end_event,
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
        seen
    from
        etl.stage_state as source
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
    '''
)

state_warehouse_insert_task << stage_state_task >> state_warehouse_update_task
