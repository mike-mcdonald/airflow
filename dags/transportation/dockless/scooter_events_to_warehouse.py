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
from airflow.operators.python_operator import BranchPythonOperator

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
    'start_date':  datetime(2019, 4, 27),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(seconds=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


providers = ['lime', 'spin', 'bolt', 'shared', 'razor', 'bird']

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
    pace = timedelta(hours=48) if datetime.now(
    ) < end_time.add(2) else timedelta(hours=2)
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
        return f'{kwargs["provider"]}_warehouse_skipped'

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

    events['batch'] = kwargs['templates_dict']['batch']
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

    return f'{kwargs["provider"]}_extract_events-sql'


api_extract_tasks = []
skip_warehouse_tasks = []
delete_data_lake_extract_tasks = []
sql_extract_tasks = []
provider_sync_tasks = []
vehicle_sync_tasks = []
stage_tasks = []
clean_extract_before_tasks = []
clean_extract_after_tasks = []
clean_stage_before_tasks = []
clean_stage_after_tasks = []
event_warehouse_update_tasks = []
event_warehouse_insert_tasks = []


# Extract data from providers and stage in tables
for provider in providers:
    dag_id = f'{provider}_events_to_warehouse'
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        catchup=True,
        schedule_interval='@hourly',
    )
    mobility_provider_conn_id = f'mobility_provider_{provider}'
    mobility_provider_token_conn_id = f'mobility_provider_{provider}_token'

    events_remote_path = f'/transportation/mobility/etl/event/{provider}-{{{{ ts_nodash }}}}.csv'

    api_extract_tasks.append(
        BranchPythonOperator(
            task_id=f'{provider}_extract_data_lake',
            dag=dag,
            depends_on_past=False,
            pool=f'{provider}_api_pool',
            provide_context=True,
            python_callable=scooter_events_to_datalake,
            op_kwargs={
                'provider': provider,
                'mobility_provider_conn_id': mobility_provider_conn_id,
                'mobility_provider_token_conn_id': mobility_provider_token_conn_id,
                'sql_conn_id': 'azure_sql_server_default',
                'data_lake_conn_id': 'azure_data_lake_default',
            },
            templates_dict={
                'batch': f'{provider}-{{{{ ts_nodash }}}}',
                'events_local_path': f'/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-{{{{ ts_nodash }}}}.csv',
                'events_remote_path': events_remote_path,
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
            }))

    skip_warehouse_tasks.append(
        DummyOperator(
            task_id=f'{provider}_warehouse_skipped',
            dag=dag,
            depends_on_past=False,
        )
    )

    delete_data_lake_extract_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f'{provider}_delete_extract',
                                    dag=dag,
                                    depends_on_past=False,
                                    azure_data_lake_conn_id='azure_data_lake_default',
                                    remote_path=events_remote_path))

    sql_extract_tasks.append(MsSqlOperator(
        task_id=f'{provider}_extract_events-sql',
        dag=dag,
        depends_on_past=False,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
        create table etl.extract_event_{provider}_{{{{ ts_nodash }}}}
        with
        (
            distribution = round_robin,
            heap
        )
        as
        select
            event_hash as hash,
            provider_id,
            provider_name,
            device_id,
            vehicle_id,
            vehicle_type,
            propulsion_type,
            date_key,
            event_time as datetime,
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
            batch = '{provider}-{{{{ ts_nodash }}}}'
        '''
    ))

    provider_sync_tasks.append(MsSqlOperator(
        task_id=f'{provider}_provider_sync',
        mssql_conn_id='azure_sql_server_full',
        dag=dag,
        pool='scooter_azure_sql_server',
        sql=f'''
        insert into
            dim.provider (
                provider_id,
                provider_name
            )
        select distinct
            provider_id,
            provider_name
        from
            etl.extract_event_{provider}_{{{{ ts_nodash }}}} as e
        where not exists (
            select
                1
            from
                dim.provider as p
            where
                p.provider_id = e.provider_id
        )
        '''
    ))

    vehicle_sync_tasks.append(MsSqlOperator(
        task_id=f'{provider}_vehicle_sync',
        mssql_conn_id='azure_sql_server_full',
        dag=dag,
        pool='scooter_azure_sql_server',
        sql=f'''
        insert into
            dim.vehicle (
                device_id,
                vehicle_id,
                vehicle_type
            )
        select distinct
            device_id,
            vehicle_id,
            vehicle_type
        from
            etl.extract_event_{provider}_{{{{ ts_nodash }}}} as e
        where not exists (
            select
                1
            from
                dim.vehicle as v
            where
                v.device_id = e.device_id
            and v.vehicle_id = e.vehicle_id
        )
        '''
    ))

    stage_tasks.append(MsSqlOperator(
        task_id=f'{provider}_stage_events',
        dag=dag,
        depends_on_past=False,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
        create table etl.stage_event_{provider}_{{{{ ts_nodash }}}}
        with
        (
            distribution = round_robin,
            heap
        )
        as
        select
            p.[key] as provider_key,
            v.[key] as vehicle_key,
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
        from
            etl.extract_event_{provider}_{{{{ ts_nodash }}}} as e
        left join
            dim.provider as p on p.provider_id = e.provider_id
        left join
            dim.vehicle as v on (
                v.device_id = e.device_id
                and v.vehicle_id = e.vehicle_id
            )
            '''
    ))

    clean_extract_before_tasks.append(MsSqlOperator(
        task_id=f'{provider}_clean_extract_table_before',
        dag=dag,
        depends_on_past=False,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
        if exists (
            select 1
            from sysobjects
            where name = 'extract_event_{provider}_{{{{ ts_nodash }}}}'
        )
        drop table etl.extract_event_{provider}_{{{{ ts_nodash }}}}
        '''
    ))

    clean_extract_after_tasks.append(MsSqlOperator(
        task_id=f'{provider}_clean_extract_table_after',
        dag=dag,
        depends_on_past=False,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
        if exists (
            select 1
            from sysobjects
            where name = 'extract_event_{provider}_{{{{ ts_nodash }}}}'
        )
        drop table etl.extract_event_{provider}_{{{{ ts_nodash }}}}
        '''
    ))

    clean_stage_before_tasks.append(MsSqlOperator(
        task_id=f'{provider}_clean_stage_table_before',
        dag=dag,
        depends_on_past=False,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
        if exists (
            select 1
            from sysobjects
            where name = 'stage_event_{provider}_{{{{ ts_nodash }}}}'
        )
        drop table etl.stage_event_{provider}_{{{{ ts_nodash }}}}
        '''
    ))

    clean_stage_after_tasks.append(MsSqlOperator(
        task_id=f'{provider}_clean_stage_table_after',
        dag=dag,
        depends_on_past=False,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
        drop table etl.stage_event_{provider}_{{{{ ts_nodash }}}}
        '''
    ))

    event_warehouse_update_tasks.append(MsSqlOperator(
        task_id=f'{provider}_warehouse_update_event',
        dag=dag,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
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
            etl.stage_event_{provider}_{{{{ ts_nodash }}}} as source
        where
            source.hash = fact.event.hash
        '''
    ))

    event_warehouse_insert_tasks.append(MsSqlOperator(
        task_id=f'{provider}_warehouse_insert_event',
        dag=dag,
        mssql_conn_id='azure_sql_server_full',
        pool='scooter_azure_sql_server',
        sql=f'''
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
            etl.stage_event_{provider}_{{{{ ts_nodash }}}} as source
        where not exists (
            select
                1
            from
                fact.event as target
            where
                target.hash = source.hash
        )
        '''
    ))

    globals()[dag_id] = dag

for i in range(len(providers)):
    clean_extract_before_tasks[i] >> sql_extract_tasks[i]
    api_extract_tasks[i] >> [sql_extract_tasks[i],
                             skip_warehouse_tasks[i]]

    sql_extract_tasks[i] >> [delete_data_lake_extract_tasks[i],
                             provider_sync_tasks[i], vehicle_sync_tasks[i]]

    clean_stage_before_tasks[i] >> stage_tasks[i]
    [vehicle_sync_tasks[i], provider_sync_tasks[i]] >> stage_tasks[i]

    stage_tasks[i] >> [event_warehouse_insert_tasks[i],
                       event_warehouse_update_tasks[i]] >> clean_stage_after_tasks[i]
    stage_tasks[i] >> clean_extract_after_tasks[i]
