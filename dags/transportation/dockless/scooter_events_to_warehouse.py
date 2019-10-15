'''
DAG for ETL Processing of Dockless Mobility Provider Data
'''
import hashlib
import json
import logging
import pathlib
import os

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from shapely.geometry import Point
from shapely.wkt import loads

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
    dag_id='scooter_events_to_warehouse',
    default_args=default_args,
    catchup=True,
    schedule_interval='@hourly',
)

providers = ['lime', 'spin', 'bolt', 'shared', 'razor', 'bird']

task1 = DummyOperator(
    task_id='provider_extract_start',
    dag=dag
)

task2 = DummyOperator(
    task_id='provider_extract_complete',
    dag=dag
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


def scooter_events_to_datalake(**kwargs):
    end_time = kwargs.get('execution_date')
    pace = timedelta(hours=48)
    start_time = end_time - pace

    # Create the hook
    hook = MobilityProviderHook(
        mobility_provider_conn_id=kwargs.get('mobility_provider_conn_id'),
        mobility_provider_token_conn_id=kwargs.get('mobility_provider_token_conn_id'))

    # Get trips as a DataFrame
    events = gpd.GeoDataFrame(hook.get_events(
        start_time=start_time, end_time=end_time))

    if len(events) <= 0:
        logging.warning(
            f'Received no events for time period {start_time} to {end_time}')
        return

    events = events.rename(index=str, columns={
        'event_type': 'state',
        'event_type_reason': 'event'
    })

    events['event_hash'] = events.apply(
        lambda x: hashlib.md5(
            f'{x.device_id}{x.event}{x.event_time}'.encode(
                'utf-8')
        ).hexdigest(), axis=1)

    events['event_time'] = events.event_time.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    events['event_time'] = events.event_time.map(lambda x: x.replace(
        tzinfo=None))  # Remove timezone info after shifting
    events['event_time'] = events.event_time.dt.round('L')
    events['date_key'] = events.event_time.map(
        lambda x: int(x.strftime('%Y%m%d')))

    event_weights = pd.DataFrame.from_dict(
        EVENT_WEIGHT, orient='index', columns=['event_weight'])

    events = events.join(event_weights, on='event')

    events = events.sort_values(by=['device_id', 'event_time', 'event_weight'])

    events['event_time'] = events.event_time.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    events = events.drop_duplicates(subset='event_hash')

    events['batch'] = kwargs.get('ts_nodash')
    events['seen'] = datetime.now()
    events['seen'] = events.seen.dt.round('L')
    events['seen'] = events.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    # Get the GeoDataFrame configured correctly
    events['event_location'] = events.event_location.map(
        lambda x: x['geometry']['coordinates'])
    events['event_location'] = events.event_location.apply(Point)
    events = events.set_geometry('event_location')
    events.crs = {'init': 'epsg:4326'}

    events['propulsion_type'] = events.propulsion_type.map(
        lambda x: ','.join(sorted(x)))

    # Aggregate the location to a cell
    hook = AzureMsSqlDataFrameHook(
        azure_mssql_conn_id=kwargs.get('sql_conn_id'))

    cells = hook.read_table_dataframe(table_name='cell', schema='dim')
    cells['geometry'] = cells.wkt.map(lambda g: loads(g))
    cells = gpd.GeoDataFrame(cells)
    cells.crs = {'init': 'epsg:4326'}

    events['cell_key'] = gpd.sjoin(
        events, cells, how='left', op='within')['key']

    del cells

    hook = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs.get('data_lake_conn_id')
    )

    def find_geospatial_dim(local_path, remote_path):
        pathlib.Path(os.path.dirname(local_path)
                     ).mkdir(parents=True, exist_ok=True)

        df = hook.download_file(
            local_path, remote_path)
        df = gpd.read_file(local_path)
        df['geometry'] = df.wkt.map(loads)
        df.crs = {'init': 'epsg:4326'}

        series = gpd.sjoin(
            events.copy(), df, how='left', op='within').drop_duplicates(subset='event_hash')['key']

        del df
        os.remove(local_path)

        return series

    with ThreadPoolExecutor(max_workers=8) as executor:
        city_key = executor.submit(find_geospatial_dim,
                                   kwargs['templates_dict']['cities_local_path'], kwargs['templates_dict']['cities_remote_path'])
        parking_district_key = executor.submit(find_geospatial_dim,
                                               kwargs['templates_dict']['parking_districts_local_path'], kwargs['templates_dict']['parking_districts_remote_path'])
        pattern_area_key = executor.submit(find_geospatial_dim,
                                           kwargs['templates_dict']['pattern_areas_local_path'], kwargs['templates_dict']['pattern_areas_remote_path'])
        census_block_group_key = executor.submit(find_geospatial_dim,
                                                 kwargs['templates_dict']['census_blocks_local_path'], kwargs['templates_dict']['census_blocks_remote_path'])
        county_key = executor.submit(find_geospatial_dim,
                                     kwargs['templates_dict']['counties_local_path'], kwargs['templates_dict']['counties_remote_path'])
        neighborhood_key = executor.submit(find_geospatial_dim,
                                           kwargs['templates_dict']['neighborhoods_local_path'], kwargs['templates_dict']['neighborhoods_remote_path'])
        park_key = executor.submit(find_geospatial_dim,
                                   kwargs['templates_dict']['parks_local_path'], kwargs['templates_dict']['parks_remote_path'])
        zipcode_key = executor.submit(find_geospatial_dim,
                                      kwargs['templates_dict']['zipcodes_local_path'], kwargs['templates_dict']['zipcodes_remote_path'])

        events['city_key'] = city_key.result()
        events['parking_district_key'] = parking_district_key.result()
        events['pattern_area_key'] = pattern_area_key.result()
        events['census_block_group_key'] = census_block_group_key.result()
        events['county_key'] = county_key.result()
        events['neighborhood_key'] = neighborhood_key.result()
        events['park_key'] = park_key.result()
        events['zipcode_key'] = zipcode_key.result()

        del city_key
        del parking_district_key
        del pattern_area_key
        del census_block_group_key
        del county_key
        del neighborhood_key
        del park_key
        del zipcode_key

    del events['event_location']

    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('events_local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    events['associated_trip'] = events['associated_trip'] if 'associated_trip' in events else np.nan

    events[[
        'event_hash',
        'provider_id',
        'provider_name',
        'device_id',
        'vehicle_id',
        'vehicle_type',
        'propulsion_type',
        'date_key',
        'event_time',
        'state',
        'event',
        'event_weight',
        'cell_key',
        'census_block_group_key',
        'city_key',
        'county_key',
        'neighborhood_key',
        'park_key',
        'parking_district_key',
        'pattern_area_key',
        'zipcode_key',
        'battery_pct',
        'associated_trip',
        'seen',
        'batch'
    ]].to_csv(kwargs.get('templates_dict').get('events_local_path'), index=False)

    hook.upload_file(kwargs.get('templates_dict').get(
        'events_local_path'), kwargs.get('templates_dict').get('events_remote_path'))

    os.remove(kwargs.get('templates_dict').get('events_local_path'))


remote_paths_delete_tasks = []

# Extract data from providers and stage in tables
for provider in providers:
    mobility_provider_conn_id = f'mobility_provider_{provider}'
    mobility_provider_token_conn_id = f'mobility_provider_{provider}_token'

    events_remote_path = f'/transportation/mobility/etl/event/{provider}-{{{{ ts_nodash }}}}.csv'

    event_extract_task = PythonOperator(
        task_id=f'loading_{provider}_events',
        dag=dag,
        provide_context=True,
        python_callable=scooter_events_to_datalake,
        op_kwargs={
            'mobility_provider_conn_id': mobility_provider_conn_id,
            'mobility_provider_token_conn_id': mobility_provider_token_conn_id,
            'sql_conn_id': 'azure_sql_server_default',
            'data_lake_conn_id': 'azure_data_lake_default',
        },
        templates_dict={
            'events_local_path': f'/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-{{{{ ts_nodash }}}}.csv',
            'events_remote_path': f'/transportation/mobility/etl/event/{provider}-{{{{ ts_nodash }}}}.csv',
            'cities_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/cities-{{ ts_nodash }}.csv',
            'cities_remote_path': '/transportation/mobility/dim/cities.csv',
            'parking_districts_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parking_districts-{{ ts_nodash }}.csv',
            'parking_districts_remote_path': '/transportation/mobility/dim/parking_districts.csv',
            'pattern_areas_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/pattern_areas-{{ ts_nodash }}.csv',
            'pattern_areas_remote_path': '/transportation/mobility/dim/pattern_areas.csv',
            'census_blocks_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/census_block_groups-{{ ts_nodash }}.csv',
            'census_blocks_remote_path': '/transportation/mobility/dim/census_block_groups.csv',
            'counties_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/counties-{{ ts_nodash }}.csv',
            'counties_remote_path': '/transportation/mobility/dim/counties.csv',
            'neighborhoods_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/neighborhoods-{{ ts_nodash }}.csv',
            'neighborhoods_remote_path': '/transportation/mobility/dim/neighborhoods.csv',
            'parks_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/parks-{{ ts_nodash }}.csv',
            'parks_remote_path': '/transportation/mobility/dim/parks.csv',
            'zipcodes_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/zipcodes-{{ ts_nodash }}.csv',
            'zipcodes_remote_path': '/transportation/mobility/dim/zipcodes.csv',
        })

    remote_paths_delete_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f'delete_{provider}_extract',
                                    dag=dag,
                                    azure_data_lake_conn_id='azure_data_lake_default',
                                    remote_path=events_remote_path))

    event_extract_task.set_upstream(task1)
    event_extract_task.set_downstream(task2)

event_extract_task = MsSqlOperator(
    task_id='extract_external_batch',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    insert into
        etl.extract_event (
            event_hash,
            provider_id,
            provider_name,
            device_id,
            vehicle_id,
            vehicle_type,
            propulsion_type,
            date_key,
            event_time,
            state,
            event,
            event_weight,
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
            seen,
            batch
        )
    select
        event_hash,
        provider_id,
        provider_name,
        device_id,
        vehicle_id,
        vehicle_type,
        propulsion_type,
        date_key,
        event_time,
        state,
        event,
        event_weight,
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
        seen,
        batch
    from
        etl.external_event
    where
        batch = '{{ ts_nodash }}'
    '''
)

task2 >> event_extract_task


provider_sync_task = MobilityProviderSyncOperator(
    task_id='provider_sync',
    source_table='etl.extract_event',
    mssql_conn_id='azure_sql_server_full',
    dag=dag
)

vehicle_sync_task = MobilityVehicleSyncOperator(
    task_id='vehicle_sync',
    source_table='etl.extract_event',
    mssql_conn_id='azure_sql_server_full',
    dag=dag
)

provider_sync_task << event_extract_task >> vehicle_sync_task

stage_event_task = MsSqlOperator(
    task_id=f'stage_events',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    insert into
        etl.stage_event (
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
            associated_trip,
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
        associated_trip,
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
        '''
)

provider_sync_task >> stage_event_task << vehicle_sync_task

clean_extract_table_task = MsSqlOperator(
    task_id='clean_extract_table',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    delete
    from
        etl.extract_event
    where
        batch = '{{ ts_nodash }}'
    '''
)

clean_extract_table_task << stage_event_task


clean_stage_before_task = MsSqlOperator(
    task_id='clean_stage_table_before',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    delete
    from
        etl.stage_event
    where
        batch = '{{ ts_nodash }}'
    '''
)

clean_stage_before_task >> stage_event_task

clean_stage_task_after = MsSqlOperator(
    task_id='clean_stage_table_after',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    delete
    from
        etl.stage_event
    where
        batch = '{{ ts_nodash }}'
    '''
)

event_warehouse_update_task = MsSqlOperator(
    task_id='warehouse_update_event',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    update
        fact.event
    set
        last_seen = source.seen,
        cell_key = source.cell_key,
        census_block_group_key = source.census_block_group_key,
        city_key = source.city_key,
        county_key = source.county_key,
        neighborhood_key = source.neighborhood_key,
        park_key = source.park_key,
        parking_district_key = source.parking_district_key,
        pattern_area_key = source.pattern_area_key,
        zipcode_key = source.zipcode_key
    from
        etl.stage_event as source
    where
        source.hash = fact.event.hash
    and source.batch = '{{ ts_nodash }}'
    '''
)

event_warehouse_insert_task = MsSqlOperator(
    task_id='warehouse_insert_event',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    insert into
        fact.event (
            provider_key,
            vehicle_key,
            propulsion_type,
            hash,
            date_key,
            datetime,
            state,
            event,
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
            first_seen,
            last_seen
        )
    select
        provider_key,
        vehicle_key,
        propulsion_type,
        hash,
        date_key,
        datetime,
        state,
        event,
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
        seen,
        seen
    from
        etl.stage_event as source
    where
        batch = '{{ ts_nodash }}'
    and not exists (
        select
            1
        from
            fact.event as target
        where
            target.hash = source.hash
    )
    '''
)

event_warehouse_insert_task << stage_event_task >> event_warehouse_update_task

event_warehouse_insert_task >> clean_stage_task_after << event_warehouse_update_task


for task in remote_paths_delete_tasks:
    event_extract_task >> task
