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
from math import atan2, pi, sqrt
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
    'start_date':  datetime(2020, 1, 1),
    'email': ['michael.mcdonald@portlandoregon.gov'],
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


def process_segment_hits_to_data_lake(**kwargs):
    end_time = kwargs['execution_date']
    pace = timedelta(hours=48) if datetime.now(
    ) < end_time.add(hours=48) else timedelta(hours=2)
    start_time = end_time - pace

    # Create the hook
    hook_api = MobilityProviderHook(
        mobility_provider_conn_id=kwargs['mobility_provider_conn_id'],
        mobility_provider_token_conn_id=kwargs['mobility_provider_token_conn_id'])

    # Get routes as a GeoDataFrame
    trips = hook_api.get_trips(
        min_end_time=start_time, max_end_time=end_time)

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

    route_df = gpd.GeoDataFrame(
        pd.concat(trips.route.values, sort=False).sort_values(
            by=['trip_id', 'timestamp'], ascending=True
        )
    ).reset_index(drop=True)
    route_df.crs = 'epsg:4326'
    route_df['datetime'] = route_df.timestamp.map(
        lambda x: datetime.fromtimestamp(x / 1000, tz=timezone('UTC')).astimezone(timezone('US/Pacific')))
    route_df['datetime'] = route_df.datetime.dt.round('H')
    route_df['datetime'] = route_df.datetime.map(
        lambda x: datetime.replace(x, tzinfo=None))
    route_df['date_key'] = route_df.datetime.map(
        lambda x: int(x.strftime('%Y%m%d')))
    # Generate a hash to aid in merge operations
    route_df['hash'] = route_df.apply(lambda x: hashlib.md5((
        x.trip_id + x.provider_id + x.datetime.strftime('%d%m%Y%H%M%S%f')
    ).encode('utf-8')).hexdigest(), axis=1)
    route_df['datetime'] = route_df.datetime.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    logging.debug("Reading segments from data warehouse...")

    hook_mssql = AzureMsSqlDataFrameHook(
        azure_mssql_conn_id=kwargs['sql_conn_id']
    )

    # Break out segment hits
    segments = hook_mssql.read_table_dataframe(
        table_name="segment", schema="dim")
    segments['geometry'] = segments.wkt.map(lambda g: loads(g))
    segments = gpd.GeoDataFrame(segments[['key', 'geometry']])
    segments.crs = 'epsg:4326'

    logging.debug("Mapping routes to segments...")

    route_df['segment_key'] = gpd.sjoin(
        route_df, segments, how="left", op="within"
    ).drop_duplicates(subset='hash')['key']

    logging.debug("Measuring route characteristics...")
    # Swtich to mercator to measure in meters
    route_df = route_df.to_crs(epsg=3857)

    del segments
    del trips

    route_df['next_timestamp'] = route_df.groupby(
        'trip_id').timestamp.shift(-1)

    route_df['x'] = route_df.geometry.map(lambda g: g.x)
    route_df['y'] = route_df.geometry.map(lambda g: g.y)
    route_df['nx'] = route_df.groupby(['trip_id']).x.shift(-1)
    route_df['ny'] = route_df.groupby(['trip_id']).y.shift(-1)

    del route_df['geometry']

    # drop destination
    route_df = route_df.dropna()

    route_df['dx'] = route_df.apply(
        lambda x: x.nx - x.x, axis=1)
    route_df['dy'] = route_df.apply(
        lambda x: x.ny - x.y, axis=1)
    route_df['dt'] = route_df.apply(
        lambda x: (x.next_timestamp - x.timestamp) / 1000, axis=1)

    del route_df['x']
    del route_df['y']
    del route_df['nx']
    del route_df['ny']
    del route_df["timestamp"]
    del route_df["next_timestamp"]

    def find_heading(hit):
        deg = atan2(hit.dx, hit.dy) / pi * 180
        if deg < 0:
            deg = deg + 360
        return deg

    def find_speed(hit):
        if hit['dt'] <= 0:
            return 0

        d = sqrt(pow((hit.dx), 2) + pow((hit.dy), 2))

        return d / hit['dt']

    route_df['heading'] = route_df.apply(find_heading, axis=1)
    route_df['speed'] = route_df.apply(find_speed, axis=1)

    del route_df['dx']
    del route_df['dy']
    del route_df['dt']

    logging.debug("Writing routes to data lake...")

    route_df = route_df.drop_duplicates(
        subset=['segment_key', 'trip_id'], keep='last')

    del route_df["trip_id"]

    pathlib.Path(os.path.dirname(kwargs['templates_dict']['segment_hits_local_path'])
                 ).mkdir(parents=True, exist_ok=True)

    route_df[[
        'provider_id',
        'date_key',
        'segment_key',
        'hash',
        'datetime',
        'vehicle_type',
        'propulsion_type',
        'heading',
        'speed',
        'seen',
        'batch'
    ]].to_csv(kwargs.get('templates_dict').get('segment_hits_local_path'), index=False)

    hook_data_lake = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['data_lake_conn_id'])

    hook_data_lake.upload_file(kwargs.get('templates_dict').get('segment_hits_local_path'),
                               kwargs.get('templates_dict').get('segment_hits_remote_path'))

    os.remove(kwargs.get('templates_dict').get('segment_hits_local_path'))

    return f'{kwargs["provider"]}_extract_external_segment_hits'


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
    dag_id = f'{provider}_segment_hits_to_warehouse'
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        catchup=True,
        schedule_interval='@hourly',
    )

    mobility_provider_conn_id = f'mobility_provider_{provider}'
    mobility_provider_token_conn_id = f'mobility_provider_{provider}_token'

    segment_hits_remote_path = f'/transportation/mobility/etl/segment_hit/{provider}-{{{{ ts_nodash }}}}.csv'

    api_extract_tasks.append(
        BranchPythonOperator(
            task_id=f'{provider}_extract_data_lake',
            dag=dag,
            depends_on_past=False,
            pool=f'{provider}_api_pool',
            provide_context=True,
            python_callable=process_segment_hits_to_data_lake,
            op_kwargs={
                'provider': provider,
                'mobility_provider_conn_id': mobility_provider_conn_id,
                'mobility_provider_token_conn_id': mobility_provider_token_conn_id,
                'sql_conn_id': 'azure_sql_server_default',
                'data_lake_conn_id': 'azure_data_lake_default',
            },
            templates_dict={
                'batch': f'{provider}-{{{{ ts_nodash }}}}',
                'segment_hits_remote_path': segment_hits_remote_path,
                'segment_hits_local_path': f'/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-segment-hits-{{{{ ts_nodash }}}}.csv'
            }))

    delete_data_lake_extract_tasks.append(
        AzureDataLakeRemoveOperator(task_id=f'{provider}_delete_segment_hit_extract',
                                    dag=dag,
                                    depends_on_past=False,
                                    azure_data_lake_conn_id='azure_data_lake_default',
                                    remote_path=segment_hits_remote_path))

    skip_warehouse_tasks.append(
        DummyOperator(
            task_id=f'{provider}_warehouse_skipped',
            dag=dag,
            depends_on_past=False,
        )
    )

    sql_extract_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_extract_external_segment_hits',
            dag=dag,
            depends_on_past=False,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            if exists (
                select 1
                from sysobjects
                where name = 'extract_segment_hit_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.extract_segment_hit_{provider}_{{{{ ts_nodash }}}}

            create table
                etl.extract_segment_hit_{provider}_{{{{ ts_nodash }}}}
            with
            (
                distribution = round_robin,
                heap
            )
            as
            select
                provider_id,
                date_key,
                segment_key,
                hash,
                datetime,
                vehicle_type,
                propulsion_type,
                heading,
                speed,
                seen,
                batch
            from
                etl.external_segment_hit
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
                where name = 'extract_segment_hit_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.extract_segment_hit_{provider}_{{{{ ts_nodash }}}}
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
                where name = 'stage_segment_hit_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.stage_segment_hit_{provider}_{{{{ ts_nodash }}}}
            '''
        ))

    stage_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_stage_segment_hits',
            dag=dag,
            depends_on_past=False,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            if exists (
                select 1
                from sysobjects
                where name = 'stage_segment_hit_{provider}_{{{{ ts_nodash }}}}'
            )
            drop table etl.stage_segment_hit_{provider}_{{{{ ts_nodash }}}}

            create table etl.stage_segment_hit_{provider}_{{{{ ts_nodash }}}}
            with
            (
                distribution = round_robin,
                heap
            )
            as
            select
                p.[key] as provider_key,
                date_key,
                segment_key,
                hash,
                datetime,
                vehicle_type,
                propulsion_type,
                heading,
                speed,
                seen,
                batch
            from
                etl.extract_segment_hit_{provider}_{{{{ ts_nodash }}}} as source
            left join
                dim.provider as p on p.provider_id = source.provider_id
            '''
        ))

    warehouse_update_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_warehouse_update_segment_hit',
            dag=dag,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            update
                fact.segment_hit
            set
                last_seen = source.seen,
                date_key = source.date_key,
                segment_key = source.segment_key,
                datetime = source.datetime,
                vehicle_type = source.vehicle_type,
                propulsion_type = source.propulsion_type,
                heading = source.heading,
                speed = source.speed
            from
                etl.stage_segment_hit_{provider}_{{{{ ts_nodash }}}} as source
            where
                source.hash = fact.segment_hit.hash
            '''
        ))

    warehouse_insert_tasks.append(
        MsSqlOperator(
            task_id=f'{provider}_warehouse_insert_segment_hit',
            dag=dag,
            mssql_conn_id='azure_sql_server_full',
            pool='scooter_azure_sql_server',
            sql=f'''
            insert into fact.segment_hit (
                provider_key,
                date_key,
                segment_key,
                hash,
                datetime,
                vehicle_type,
                propulsion_type,
                heading,
                speed,
                first_seen,
                last_seen
            )
            select
                provider_key,
                date_key,
                segment_key,
                hash,
                datetime,
                vehicle_type,
                propulsion_type,
                heading,
                speed,
                seen,
                seen
            from
                etl.stage_segment_hit_{provider}_{{{{ ts_nodash }}}} as source
            where not exists (
                select
                    1
                from
                    fact.segment_hit as target
                where
                    target.hash = source.hash
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
