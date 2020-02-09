'''
DAG for ETL Processing of Dockless Mobility Provider Data
'''
import hashlib
import json
import logging
import pathlib
import os
import uuid

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from shapely.geometry import Point
from shapely.wkb import loads

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
    'start_date': datetime(2018, 4, 1),
    'end_date': datetime(2018, 12, 31)
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
    nd_time = kwargs['execution_date']
    pace = timedelta(hours=24)
    start_time = end_time - pace

    # Create the hook
    hook_pgsql = PgSqlDataFrameHook(
        pgsql_conn_id='pgsql_scooter_pilot_1'
    )

    # Get trips as a GeoDataFrame
    trips = hook_pgsql.read_sql(f'''
        select
            t.key,
            t.alternatekey,
            c.name as provider_name,
            v.key as vehicle_key,
            v.name as vehicle_id,
            sd.date as start_date,
            t.starttime as start_time,
            ed.date as end_date,
            t.endtime as end_time,
            t.startpoint as origin,
            t.endpoint as destination,
            t.distance,
            t.duration,
            t.accuracy,
            t.standardcost as standard_cost,
            t.actualcost as actual_cost,
            t.parkingverification as parking_verification_url
        from
            fact.trip as t
        inner join
            dim.company as c on c.key = t.companykey
        inner join
            dim.vehicle as v on v.key = t.vehiclekey
        inner join
            dim.calendar as sd on sd.key = t.startdatekey
        inner join
            dim.calendar as ed on ed.key = t.enddatekey
        where
            sd.date between '{start_time.strftime('%Y-%m-%d')}' and '{end_time.strftime('%Y-%m-%d')}'
        and
            t.route is not null
        and
            t.citystartkey is not null
        and
            t.cityendkey is not null
    ''')

    if len(trips) <= 0:
        logging.warning(
            f'Received no trips for time period {start_time} to {end_time}')
        return 'warehouse_skipped'

    trips['trip_id'] = trips.key.map(lambda x: str(uuid.uuid4()))
    trips['propulsion_type'] = 'electric,human'

    trips['batch'] = kwargs.get('templates_dict').get('batch')
    trips['seen'] = datetime.now()
    trips['seen'] = trips.seen.dt.round('L')
    trips['seen'] = trips.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    trips['start_time'] = trips.apply(
        lambda x: x.start_date + x.start_time, axis=1)
    trips['start_time'] = trips.start_time.map(
        lambda x: datetime.replace(x, tzinfo=None))  # Remove timezone info after shifting
    trips['start_time'] = trips.start_time.dt.round('L')

    trips['start_date_key'] = trips.start_time.map(
        lambda x: int(x.strftime('%Y%m%d')))
    trips['start_time'] = trips.start_time.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    trips['end_time'] = trips.apply(lambda x: x.end_date + x.end_time, axis=1)
    trips['end_time'] = trips.end_time.map(
        lambda x: datetime.replace(x, tzinfo=None))
    trips['end_time'] = trips.end_time.dt.round('L')
    trips['end_date_key'] = trips.end_time.map(
        lambda x: int(x.strftime('%Y%m%d')))
    trips['end_time'] = trips.end_time.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    trips['origin'] = trips.origin.map(lambda x: loads(x, hex=True))
    trips['destination'] = trips.destination.map(lambda x: loads(x, hex=True))

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

    return 'extract_external_trips'


dag = DAG(
    dag_id='scooter_pilot_1_trips_to_warehouse',
    default_args=default_args,
    catchup=True,
    schedule_interval='@daily',
)

provider = 'pilot_1'

trips_remote_path = f'/transportation/mobility/etl/trip/pilot_1/{provider}_{{{{ ts_nodash }}}}.csv'

api_extract_tasks = BranchPythonOperator(
    task_id=f'extract_data_lake',
    dag=dag,
    depends_on_past=False,
    pool=f'api_pool',
    provide_context=True,
    python_callable=process_trips_to_data_lake,
    op_kwargs={
        'sql_conn_id': 'azure_sql_server_default',
        'data_lake_conn_id': 'azure_data_lake_default',
    },
    templates_dict={
        'batch': f'{provider}_{{{{ ts_nodash }}}}',
        'trips_local_path': f'/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/trips-{provider}_{{{{ ts_nodash }}}}.csv',
        'trips_remote_path': trips_remote_path,
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
    },)

delete_data_lake_extract_task = AzureDataLakeRemoveOperator(
    task_id=f'delete_trip_extract',
    dag=dag,
    depends_on_past=False,
    azure_data_lake_conn_id='azure_data_lake_default',
    remote_path=trips_remote_path
)

skip_warehouse_task = DummyOperator(
    task_id='warehouse_skipped',
    dag=dag,
    depends_on_past=False,
)

sql_extract_task = MsSqlOperator(
    task_id='extract_external_trips',
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
            batch = '{provider}_{{{{ ts_nodash }}}}'
        '''
)

clean_extract_task = MsSqlOperator(
    task_id='clean_extract_table',
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
)

clean_stage_task = MsSqlOperator(
    task_id='clean_stage_table',
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
)

stage_task = MsSqlOperator(
    task_id='stage_trips',
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
)

warehouse_update_task = MsSqlOperator(
    task_id='warehouse_update_trip',
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
)

warehouse_insert_task = MsSqlOperator(
    task_id='warehouse_insert_trip',
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
)


api_extract_task >> [sql_extract_task,
                     skip_warehouse_task]

sql_extract_task >> [delete_data_lake_extract_task, stage_task]

stage_task >> clean_extract_task
stage_task >> [warehouse_insert_task,
               warehouse_update_task] >> clean_stage_task
