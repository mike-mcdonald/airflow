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

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2019, 4, 26),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(seconds=10),
    'concurrency': 1,
    'max_active_runs': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def process_trips_to_data_lake(**kwargs):
    end_time = kwargs['execution_date']
    pace = timedelta(hours=48) if datetime.now(
    ) < end_time.add(hours=48) else timedelta(hours=2)
    start_time = end_time - pace

    # Create the hook
    hook_api = MobilityProviderHook(
        mobility_provider_conn_id=kwargs['mobility_provider_conn_id'],
        mobility_provider_token_conn_id=kwargs['mobility_provider_token_conn_id'])

    # Get trips as a GeoDataFrame
    trips = gpd.GeoDataFrame(hook_api.get_trips(
        min_end_time=start_time, max_end_time=end_time))
    trips.crs = {'init': 'epsg:4326'}

    if len(trips) <= 0:
        logging.warning(
            f'Received no trips for time period {start_time} to {end_time}')
        return f'{kwargs["provider"]}_warehouse_skipped'

    trips = trips.drop_duplicates(subset=['trip_id'])

    trips = trips.rename(index=str, columns={
        'trip_duration': 'duration',
        'trip_distance': 'distance'
    })

    trips['batch'] = kwargs['templates_dict']['batch']
    trips['seen'] = datetime.now()
    trips['seen'] = trips.seen.dt.round('L')
    trips['seen'] = trips.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    trips['propulsion_type'] = trips.propulsion_type.map(
        lambda x: ','.join(sorted(x)))
    trips['start_time'] = trips.start_time.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    trips['start_time'] = trips.start_time.map(
        lambda x: datetime.replace(x, tzinfo=None))  # Remove timezone info after shifting
    trips['start_time'] = trips.start_time.dt.round('L')

    trips['start_date_key'] = trips.start_time.map(
        lambda x: int(x.strftime('%Y%m%d')))
    trips['start_time'] = trips.start_time.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    trips['end_time'] = trips.end_time.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    trips['end_time'] = trips.end_time.map(
        lambda x: datetime.replace(x, tzinfo=None))
    trips['end_time'] = trips.end_time.dt.round('L')
    trips['end_date_key'] = trips.end_time.map(
        lambda x: int(x.strftime('%Y%m%d')))
    trips['end_time'] = trips.end_time.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    logging.debug('Converting route to a GeoDataFrame...')
    # Convert the route to a DataFrame now to make mapping easier
    trips['route'] = trips.route.map(
        lambda r: gpd.GeoDataFrame.from_features(r['features']))

    def parse_route(trip):
        frame = trip.route
        frame['batch'] = trip.batch
        frame['trip_id'] = trip.trip_id
        frame['provider_id'] = trip.provider_id
        frame['vehicle_type'] = trip.vehicle_type
        frame['propulsion_type'] = trip.propulsion_type
        frame['seen'] = trip.seen
        return frame

    trips['route'] = trips.apply(parse_route, axis=1)
    # remove all rows for which the value of geometry is NaN
    trips['route'] = trips.route.map(
        lambda x: x.dropna(axis=0, subset=['geometry']))
    logging.debug('Retrieving origin and destination...')

    def get_origin(route):
        return route.loc[route['timestamp'].idxmin()].geometry

    def get_destination(route):
        return route.loc[route['timestamp'].idxmax()].geometry or route.loc[route['timestamp'].idxmin()].geometry
    # Pull out the origin and destination
    trips['origin'] = trips.route.map(get_origin)
    trips['destination'] = trips.route.map(get_destination)

    # delete before passing to dataframe write, segmentation fault otherwise
    del trips['route']

    hook_mssql = AzureMsSqlDataFrameHook(
        azure_mssql_conn_id=kwargs['sql_conn_id']
    )

    logging.debug('Reading cells from data warehouse...')

    # Map to cells

    logging.debug('Mapping trip O/D to cells...')

    hook_data_lake = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['data_lake_conn_id']
    )

    def download_data_lake_geodataframe(local_path, remote_path):
        pathlib.Path(os.path.dirname(local_path)
                     ).mkdir(parents=True, exist_ok=True)

        df = hook_data_lake.download_file(
            local_path, remote_path)
        df = gpd.read_file(local_path)
        df['geometry'] = df.wkt.map(loads)
        df.crs = {'init': 'epsg:4326'}

        return df

    def find_geospatial_dim(right_df):
        series = gpd.sjoin(
            trips, right_df, how='left', op='within').drop_duplicates(subset='trip_id')['key']

        return series

    with ThreadPoolExecutor(max_workers=8) as executor:
        cities = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['cities_local_path'], kwargs['templates_dict']['cities_remote_path'])
        parking_districts = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['parking_districts_local_path'], kwargs['templates_dict']['parking_districts_remote_path'])
        pattern_areas = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['pattern_areas_local_path'], kwargs['templates_dict']['pattern_areas_remote_path'])
        census_blocks = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['census_blocks_local_path'], kwargs['templates_dict']['census_blocks_remote_path'])
        counties = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['counties_local_path'], kwargs['templates_dict']['counties_remote_path'])
        neighborhoods = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['neighborhoods_local_path'], kwargs['templates_dict']['neighborhoods_remote_path'])
        parks = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['parks_local_path'], kwargs['templates_dict']['parks_remote_path'])
        zipcodes = executor.submit(
            download_data_lake_geodataframe, kwargs['templates_dict']['zipcodes_local_path'], kwargs['templates_dict']['zipcodes_remote_path'])

        cells = hook_mssql.read_table_dataframe(
            table_name='cell', schema='dim')
        cells['geometry'] = cells.wkt.map(loads)
        cells = gpd.GeoDataFrame(cells)
        cells.crs = {'init': 'epsg:4326'}

        trips = trips.set_geometry('origin')

        start_cell_key = executor.submit(
            find_geospatial_dim, cells)
        start_city_key = executor.submit(
            find_geospatial_dim, cities.result())
        start_parking_district_key = executor.submit(
            find_geospatial_dim, parking_districts.result())
        start_pattern_area_key = executor.submit(
            find_geospatial_dim, pattern_areas.result())
        start_census_block_group_key = executor.submit(
            find_geospatial_dim, census_blocks.result())
        start_counties_key = executor.submit(
            find_geospatial_dim, counties.result())
        start_neighborhoods_key = executor.submit(
            find_geospatial_dim, neighborhoods.result())
        start_parks_key = executor.submit(
            find_geospatial_dim, parks.result())
        start_zipcodes_key = executor.submit(
            find_geospatial_dim, zipcodes.result())

        trips['start_cell_key'] = start_cell_key.result()
        trips['start_city_key'] = start_city_key.result()
        trips['start_parking_district_key'] = start_parking_district_key.result()
        trips['start_pattern_area_key'] = start_pattern_area_key.result()
        trips['start_census_block_group_key'] = start_census_block_group_key.result()
        trips['start_county_key'] = start_counties_key.result()
        trips['start_neighborhood_key'] = start_neighborhoods_key.result()
        trips['start_park_key'] = start_parks_key.result()
        trips['start_zipcode_key'] = start_zipcodes_key.result()

        del start_cell_key
        del start_city_key
        del start_parking_district_key
        del start_pattern_area_key

        # New Geometry
        del start_census_block_group_key
        del start_counties_key
        del start_neighborhoods_key
        del start_parks_key
        del start_zipcodes_key

        trips = trips.set_geometry('destination')

        end_cell_key = executor.submit(
            find_geospatial_dim, cells)
        end_city_key = executor.submit(
            find_geospatial_dim, cities.result())
        end_parking_district_key = executor.submit(
            find_geospatial_dim, parking_districts.result())
        end_pattern_area_key = executor.submit(
            find_geospatial_dim, pattern_areas.result())
        end_census_block_group_key = executor.submit(
            find_geospatial_dim, census_blocks.result())
        end_counties_key = executor.submit(
            find_geospatial_dim, counties.result())
        end_neighborhoods_key = executor.submit(
            find_geospatial_dim, neighborhoods.result())
        end_parks_key = executor.submit(
            find_geospatial_dim, parks.result())
        end_zipcodes_key = executor.submit(
            find_geospatial_dim, zipcodes.result())

        trips['end_cell_key'] = end_cell_key.result()
        trips['end_city_key'] = end_city_key.result()
        trips['end_parking_district_key'] = end_parking_district_key.result()
        trips['end_pattern_area_key'] = end_pattern_area_key.result()
        trips['end_census_block_group_key'] = end_census_block_group_key.result()
        trips['end_county_key'] = end_counties_key.result()
        trips['end_neighborhood_key'] = end_neighborhoods_key.result()
        trips['end_park_key'] = end_parks_key.result()
        trips['end_zipcode_key'] = end_zipcodes_key.result()

        del end_cell_key
        del end_city_key
        del end_parking_district_key
        del end_pattern_area_key
        del end_census_block_group_key
        del end_counties_key
        del end_neighborhoods_key
        del end_parks_key
        del end_zipcodes_key

        del cells

        os.remove(kwargs['templates_dict']['cities_local_path'])
        os.remove(kwargs['templates_dict']['parking_districts_local_path'])
        os.remove(kwargs['templates_dict']['pattern_areas_local_path'])
        os.remove(kwargs['templates_dict']['census_blocks_local_path'])
        os.remove(kwargs['templates_dict']['counties_local_path'])
        os.remove(kwargs['templates_dict']['neighborhoods_local_path'])
        os.remove(kwargs['templates_dict']['parks_local_path'])
        os.remove(kwargs['templates_dict']['zipcodes_local_path'])

    logging.debug('Writing trips extract to data lake...')

    pathlib.Path(os.path.dirname(kwargs['templates_dict']['trips_local_path'])
                 ).mkdir(parents=True, exist_ok=True)

    trips['standard_cost'] = trips['standard_cost'] if 'standard_cost' in trips else np.nan
    trips['actual_cost'] = trips['actual_cost'] if 'actual_cost' in trips else np.nan
    trips['parking_verification_url'] = trips['parking_verification_url'] if 'parking_verification_url' in trips else np.nan

    trips[[
        'trip_id',
        'provider_id',
        'provider_name',
        'device_id',
        'vehicle_id',
        'vehicle_type',
        'propulsion_type',
        'start_time',
        'start_date_key',
        'start_cell_key',
        'start_census_block_group_key',
        'start_city_key',
        'start_county_key',
        'start_neighborhood_key',
        'start_park_key',
        'start_parking_district_key',
        'start_pattern_area_key',
        'start_zipcode_key',
        'end_time',
        'end_date_key',
        'end_cell_key',
        'end_census_block_group_key',
        'end_city_key',
        'end_county_key',
        'end_neighborhood_key',
        'end_park_key',
        'end_parking_district_key',
        'end_pattern_area_key',
        'end_zipcode_key',
        'distance',
        'duration',
        'accuracy',
        'standard_cost',
        'actual_cost',
        'parking_verification_url',
        'seen',
        'batch'
    ]].to_csv(kwargs['templates_dict']['trips_local_path'], index=False)

    hook_data_lake.upload_file(
        kwargs['templates_dict']['trips_local_path'], kwargs['templates_dict']['trips_remote_path'])

    os.remove(kwargs['templates_dict']['trips_local_path'])

    return f'{kwargs["provider"]}_extract_external_trips'


providers = ['lime', 'spin', 'bolt', 'shared', 'razor', 'bird']

api_extract_tasks = []
skip_warehouse_tasks = []
delete_data_lake_extract_tasks = []
sql_extract_tasks = []
stage_tasks = []
clean_extract_tasks = []
clean_stage_tasks = []
warehouse_update_tasks = []
warehouse_insert_tasks = []

# Extract data from providers and stage in tables
for provider in providers:
    dag_id = f'{provider}_trips_to_warehouse'
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        catchup=True,
        schedule_interval='@hourly',
    )

    mobility_provider_conn_id = f'mobility_provider_{provider}'
    mobility_provider_token_conn_id = f'mobility_provider_{provider}_token'

    trips_remote_path = f'/transportation/mobility/etl/trip/{provider}-{{{{ ts_nodash }}}}.csv'

    api_extract_tasks.append(
        BranchPythonOperator(
            task_id=f'{provider}_extract_data_lake',
            dag=dag,
            depends_on_past=False,
            pool=f'{provider}_api_pool',
            provide_context=True,
            python_callable=process_trips_to_data_lake,
            op_kwargs={
                'provider': provider,
                'mobility_provider_conn_id': mobility_provider_conn_id,
                'mobility_provider_token_conn_id': mobility_provider_token_conn_id,
                'sql_conn_id': 'azure_sql_server_default',
                'data_lake_conn_id': 'azure_data_lake_default',
            },
            templates_dict={
                'batch': f'{provider}-{{{{ ts_nodash }}}}',
                'trips_local_path': f'/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-trips-{{{{ ts_nodash }}}}.csv',
                'trips_remote_path': trips_remote_path,
                'segment_hits_local_path': f'/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-segment-hits-{{{{ ts_nodash }}}}.csv',
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
            },))

    delete_data_lake_extract_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f'{provider}_delete_trip_extract',
                                    dag=dag,
                                    depends_on_past=False,
                                    azure_data_lake_conn_id='azure_data_lake_default',
                                    remote_path=trips_remote_path))

    skip_warehouse_tasks.append(
        DummyOperator(
            task_id=f'{provider}_warehouse_skipped',
            dag=dag,
            depends_on_past=False,
        )
    )

    sql_extract_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_extract_external_trips',
            dag=dag,
            depends_on_past=False,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            if exists (
                select 1
                from sysobjects
                where name = 'extract_trip_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.extract_trip_{provider}_{{{{ ts_nodash }}}}

            create table
                etl.extract_trip_{provider}_{{{{ ts_nodash }}}}
            with
            (
                distribution = round_robin,
                heap
            )
            as
            select
                trip_id,
                provider_id,
                provider_name,
                device_id,
                vehicle_id,
                vehicle_type,
                propulsion_type,
                start_time,
                start_date_key,
                start_cell_key,
                start_census_block_group_key,
                start_city_key,
                start_county_key,
                start_neighborhood_key,
                start_park_key,
                start_parking_district_key,
                start_pattern_area_key,
                start_zipcode_key,
                end_time,
                end_date_key,
                end_cell_key,
                end_census_block_group_key,
                end_city_key,
                end_county_key,
                end_neighborhood_key,
                end_park_key,
                end_parking_district_key,
                end_pattern_area_key,
                end_zipcode_key,
                distance,
                duration,
                accuracy,
                standard_cost,
                actual_cost,
                parking_verification_url,
                seen,
                batch
            from
                etl.external_trip
            where
                batch = '{provider}-{{{{ ts_nodash }}}}'
            '''
        ))

    clean_extract_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_clean_extract_table',
            dag=dag,
            depends_on_past=False,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            if exists (
                select 1
                from sysobjects
                where name = 'extract_trip_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.extract_trip_{provider}_{{{{ ts_nodash }}}}
            '''
        ))

    clean_stage_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_clean_stage_table',
            dag=dag,
            depends_on_past=False,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            if exists (
                select 1
                from sysobjects
                where name = 'stage_trip_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.stage_trip_{provider}_{{{{ ts_nodash }}}}
            '''
        ))

    stage_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_stage_trips',
            dag=dag,
            depends_on_past=False,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            if exists (
                select 1
                from sysobjects
                where name = 'stage_trip_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.stage_trip_{provider}_{{{{ ts_nodash }}}}

            create table etl.stage_trip_{provider}_{{{{ ts_nodash }}}}
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
                trip_id,
                start_cell_key,
                start_census_block_group_key,
                start_city_key,
                start_county_key,
                start_neighborhood_key,
                start_park_key,
                start_parking_district_key,
                start_pattern_area_key,
                start_zipcode_key,
                end_cell_key,
                end_census_block_group_key,
                end_city_key,
                end_county_key,
                end_neighborhood_key,
                end_park_key,
                end_parking_district_key,
                end_pattern_area_key,
                end_zipcode_key,
                start_time,
                start_date_key,
                end_time,
                end_date_key,
                distance,
                duration,
                accuracy,
                standard_cost,
                actual_cost,
                parking_verification_url,
                seen,
                batch
            from
                etl.extract_trip_{provider}_{{{{ ts_nodash }}}} as source
            left join
                dim.provider as p on p.provider_id = source.provider_id
            left join
                dim.vehicle as v on (
                    v.vehicle_id = source.vehicle_id
                    and v.device_id = source.device_id
                )
            '''
        ))

    warehouse_update_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_warehouse_update_trip',
            dag=dag,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            update
                fact.trip
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
                etl.stage_trip_{provider}_{{{{ ts_nodash }}}} as source
            where
                source.trip_id = fact.trip.trip_id
            '''
        ))

    warehouse_insert_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_warehouse_insert_trip',
            dag=dag,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            insert into fact.trip (
                trip_id,
                provider_key,
                vehicle_key,
                propulsion_type,
                start_time,
                start_date_key,
                start_cell_key,
                start_census_block_group_key,
                start_city_key,
                start_county_key,
                start_neighborhood_key,
                start_park_key,
                start_parking_district_key,
                start_pattern_area_key,
                start_zipcode_key,
                end_time,
                end_date_key,
                end_cell_key,
                end_census_block_group_key,
                end_city_key,
                end_county_key,
                end_neighborhood_key,
                end_park_key,
                end_parking_district_key,
                end_pattern_area_key,
                end_zipcode_key,
                distance,
                duration,
                accuracy,
                standard_cost,
                actual_cost,
                parking_verification_url,
                first_seen,
                last_seen
            )
            select
                trip_id,
                provider_key,
                vehicle_key,
                propulsion_type,
                start_time,
                start_date_key,
                start_cell_key,
                start_census_block_group_key,
                start_city_key,
                start_county_key,
                start_neighborhood_key,
                start_park_key,
                start_parking_district_key,
                start_pattern_area_key,
                start_zipcode_key,
                end_time,
                end_date_key,
                end_cell_key,
                end_census_block_group_key,
                end_city_key,
                end_county_key,
                end_neighborhood_key,
                end_park_key,
                end_parking_district_key,
                end_pattern_area_key,
                end_zipcode_key,
                distance,
                duration,
                accuracy,
                standard_cost,
                actual_cost,
                parking_verification_url,
                seen,
                seen
            from
                etl.stage_trip_{provider}_{{{{ ts_nodash }}}} as source
            where not exists (
                select
                    1
                from
                    fact.trip as target
                where
                    target.trip_id = source.trip_id
            )
            '''
        ))

    globals()[dag_id] = dag

for i in range(len(providers)):
    api_extract_tasks[i] >> [sql_extract_tasks[i],
                             skip_warehouse_tasks[i]]

    sql_extract_tasks[i] >> [delete_data_lake_extract_tasks[i], stage_tasks[i]]

    stage_tasks[i] >> clean_extract_tasks[i]
    stage_tasks[i] >> [warehouse_insert_tasks[i],
                       warehouse_update_tasks[i]] >> clean_stage_tasks[i]
