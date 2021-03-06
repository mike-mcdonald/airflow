'''
DAG for ETL Processing of PDX GIS Open Data Counties, from Metro
'''
import hashlib
import joblib
import json
import logging
import os
import pathlib
import pickle
import time

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from math import atan2, fabs, pi, pow, sqrt
from multiprocessing import cpu_count, Pool

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from requests import Session
from shapely.geometry import Point
from shapely.wkt import dumps

import airflow

from airflow import DAG

from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.mobility_plugin import MobilityProviderHook
from airflow.hooks.mobility_plugin import SharedStreetsAPIHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator
from airflow.operators.mssql_plugin import MsSqlOperator

default_args = {
    'owner': 'airflow',
    'start_date':  datetime(2019, 4, 26),
    'email': ['michael.mcdonald@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
}

dag = DAG(
    dag_id='scooter_shst_hits_to_warehouse',
    default_args=default_args,
    catchup=True,
    schedule_interval='@hourly',
    max_active_runs=3,
)

providers = ['lime', 'spin', 'bolt', 'shared', 'razor', 'bird']


def extract_shst_hits_datalake(**kwargs):
    # lookback a week and get the last hour
    end_time = kwargs.get('execution_date') - timedelta(days=7)
    start_time = end_time - timedelta(hours=1)

    trips = []
    for provider in providers:
        hook = MobilityProviderHook(
            mobility_provider_conn_id=f'mobility_provider_{provider}',
            mobility_provider_token_conn_id=f'mobility_provider_{provider}_token'
        )

        trips.append(hook.get_trips(
            min_end_time=start_time, max_end_time=end_time))

    # Get trips as a GeoDataFrame
    trips = gpd.GeoDataFrame(pd.concat(trips))
    trips.crs = {'init': 'epsg:4326'}

    if len(trips) <= 0:
        logging.warning(
            f'Received no trips for time period {start_time} to {end_time}')
        return 'warehouse_skipped'

    hook_datalake = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs.get('azure_datalake_conn_id'))

    hook_datalake.download_file(
        kwargs['templates_dict']['kmeans_local_path'], '/transportation/mobility/learning/kmeans.pkl',)

    kmeans = joblib.load(kwargs['templates_dict']['kmeans_local_path'])

    os.remove(kwargs['templates_dict']['kmeans_local_path'])

    # Convert the route to a DataFrame now to make mapping easier
    trips['route'] = trips.route.map(lambda x: x['features'])
    trips['propulsion_type'] = trips.propulsion_type.map(
        lambda x: ','.join(sorted(x)))

    lens = [len(item) for item in trips['route']]

    route_df = pd.DataFrame({
        "trip_id": np.repeat(trips['trip_id'].values, lens),
        "provider_id": np.repeat(trips['provider_id'].values, lens),
        "device_id": np.repeat(trips['device_id'].values, lens),
        "vehicle_type": np.repeat(trips['vehicle_type'].values, lens),
        "propulsion_type": np.repeat(trips['propulsion_type'].values, lens),
        "feature": np.concatenate(trips['route'].values)
    })

    route_df['timestamp'] = route_df.feature.map(
        lambda x: x['properties']['timestamp'])
    route_df['coordinates'] = route_df.feature.map(
        lambda x: x['geometry']['coordinates'])
    route_df['geometry'] = route_df.feature.map(
        lambda x: Point(x['geometry']['coordinates']))

    route_df = gpd.GeoDataFrame(route_df.sort_values(
        by=['trip_id', 'timestamp'], ascending=True
    ).reset_index(drop=True).copy())
    route_df.crs = {'init': 'epsg:4326'}
    route_df['datetime'] = route_df.timestamp.map(
        lambda x: datetime.fromtimestamp(x / 1000, tz=timezone('UTC')).astimezone(timezone('US/Pacific')))
    route_df['datetime'] = route_df.datetime.dt.round('H')
    route_df['datetime'] = route_df.datetime.map(
        lambda x: datetime.replace(x, tzinfo=None))
    route_df['date_key'] = route_df.datetime.map(
        lambda x: int(x.strftime('%Y%m%d')))
    # Generate a hash to aid in merge operations
    route_df['hash'] = route_df.apply(lambda x: hashlib.md5((
        f'{x.trip_id}{x.device_id}{x.provider_id}{x.timestamp}{os.environ["HASH_SALT"]}'
    ).encode('utf-8')).hexdigest(), axis=1)
    route_df['datetime'] = route_df.datetime.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    coord_array = []
    route_df.coordinates.map(lambda x: coord_array.append([x[0], x[1]]))
    coord_array = np.array(coord_array)
    route_df['cluster'] = kmeans.predict(coord_array)

    shst_df = route_df[['hash', 'geometry', 'cluster']].groupby('cluster').apply(
        lambda x: {
            'type': 'FeatureCollection',
            'features': x.apply(lambda x: {
                'type': 'Feature',
                'properties': {
                    'hash': x.hash
                },
                'geometry': {
                    'type': x.geometry.geom_type,
                    'coordinates': np.array(x.geometry).tolist()
                }
            }, axis=1).values.tolist()
        })
    hook_shst = SharedStreetsAPIHook(shst_api_conn_id='shst_api_default')
    cores = cpu_count()  # Number of CPU cores on your system
    executor = ThreadPoolExecutor(max_workers=cores*4)
    shst = shst_df.map(lambda x: executor.submit(
        hook_shst.match, 'point', 'bike', x))

    route_df = route_df.to_crs(epsg=3857)

    route_df['x'] = route_df.geometry.map(lambda g: g.x)
    route_df['y'] = route_df.geometry.map(lambda g: g.y)

    route_by_trip = route_df.groupby(['trip_id'])

    route_df['nt'] = route_by_trip.timestamp.shift(-1)
    route_df['nx'] = route_by_trip.x.shift(-1)
    route_df['ny'] = route_by_trip.y.shift(-1)

    # drop destination
    route_df = route_df.dropna().copy()

    route_df['dx'] = route_df.nx - route_df.x
    route_df['dy'] = route_df.ny - route_df.y
    route_df['dt'] = (route_df.nt - route_df.timestamp) / 1000

    def find_bearing(hit):
        return atan2(hit.dx, hit.dy) / pi * 180

    def find_speed(hit):
        if hit['dt'] <= 0:
            return 0

        d = sqrt(pow((hit.dx), 2) + pow((hit.dy), 2))

        return d / hit['dt']

    route_df['bearing'] = route_df.apply(find_bearing, axis=1)
    route_df['speed'] = route_df.apply(find_speed, axis=1)

    def safe_result(x):
        try:
            return x.result()
        except:
            logging.error(
                'Error retrieving sharedstreets references, returning empty results...')
            return {'features': []}

    shst_df = shst.map(safe_result)

    shst_df = pd.DataFrame({
        'feature': np.concatenate(shst_df.map(lambda x: x['features']).values)
    })

    shst_df['hash'] = shst_df.feature.map(lambda x: x['properties']['hash'])
    shst_df['candidates'] = shst_df.feature.map(
        lambda x: x['properties']['shstCandidates'])

    route_df = route_df.merge(shst_df, on='hash')

    if len(route_df['candidates'].values) == 0:
        logging.warning(
            f'Received no candidates for time period {start_time} to {end_time}')
        return 'warehouse_skipped'

    lens = [len(item) for item in route_df['candidates']]
    shst_df = pd.DataFrame({
        "date_key": np.repeat(route_df['date_key'].values, lens),
        "hash": np.repeat(route_df['hash'].values, lens),
        "datetime": np.repeat(route_df['datetime'].values, lens),
        "trip_id": np.repeat(route_df['trip_id'].values, lens),
        "provider_id": np.repeat(route_df['provider_id'].values, lens),
        "vehicle_type": np.repeat(route_df['vehicle_type'].values, lens),
        "propulsion_type": np.repeat(route_df['propulsion_type'].values, lens),
        "bearing": np.repeat(route_df['bearing'].values, lens),
        "speed": np.repeat(route_df['speed'].values, lens),
        "candidate": np.concatenate(route_df['candidates'].values),
    })

    del route_df

    shst_df['shstBearing'] = shst_df.candidate.map(lambda x: x['bearing'])
    shst_df['shst_geometry_id'] = shst_df.candidate.map(
        lambda x: x['geometryId'])
    shst_df['shst_reference_id'] = shst_df.candidate.map(
        lambda x: x['referenceId'])
    shst_df['score'] = shst_df.candidate.map(
        lambda x: x['score'] if 'score' in x else 0)

    def normalizeAngle(angle):
        if angle < 0:
            angle = angle + 360
        return angle

    shst_df['bearing'] = shst_df.bearing.apply(normalizeAngle)
    shst_df['shstBearing'] = shst_df.shstBearing.apply(normalizeAngle)
    shst_df['bearing_diff'] = shst_df.bearing - shst_df.shstBearing
    shst_df['bearing_diff'] = shst_df.bearing_diff.apply(fabs)

    shst_df['batch'] = kwargs.get('ts_nodash')
    shst_df['seen'] = datetime.now()
    shst_df['seen'] = shst_df.seen.dt.round('L')
    shst_df['seen'] = shst_df.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    is_stopped = shst_df['speed'] == 0

    shst_df[~is_stopped].sort_values(
        by=['hash', 'bearing_diff', 'score']
    ).drop_duplicates(
        subset=['hash'],
        keep='first'
    ).drop_duplicates(
        subset=['trip_id', 'shst_geometry_id'],
        keep='last'
    )[[
        'provider_id',
        'date_key',
        'shst_geometry_id',
        'shst_reference_id',
        'hash',
        'datetime',
        'vehicle_type',
        'propulsion_type',
        'bearing',
        'speed',
        'seen',
        'batch'
    ]].to_csv(kwargs.get('templates_dict').get('local_path'), index=False)

    hook_datalake.upload_file(kwargs.get('templates_dict').get(
        'local_path'), kwargs.get('templates_dict').get('remote_path'))

    os.remove(kwargs.get('templates_dict').get('local_path'))

    return 'stage_shst_segment_hits'


parse_datalake_files_task = BranchPythonOperator(
    task_id='extract_routes_to_data_lake',
    dag=dag,
    provide_context=True,
    python_callable=extract_shst_hits_datalake,
    templates_dict={
        'kmeans_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/kmeans-{{ ts_nodash }}.pkl',
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/mobility/etl/shst_hits/{{ ts_nodash }}.csv',
        'kmeans_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/kmeans-{{ ts_nodash }}.pkl',
    },
    op_kwargs={
        'azure_datalake_conn_id': 'azure_data_lake_default'
    },
)

skip_warehouse_task = DummyOperator(
    task_id=f'warehouse_skipped',
    dag=dag,
    depends_on_past=False,
)

# Run SQL scripts to transform extract data into staged facts
stage_shst_segment_hit_task = MsSqlOperator(
    task_id=f'stage_shst_segment_hits',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    pool='scooter_azure_sql_server',
    sql='''
    if exists (
        select 1
        from sysobjects
        where name = 'stage_shst_segment_hit_{{ ts_nodash }}'
    )
    drop table etl.stage_shst_segment_hit_{{ ts_nodash }}

    create table etl.stage_shst_segment_hit_{{ ts_nodash }}
    with
    (
        distribution = round_robin,
        heap
    )
    as
    select
        p.[key] as provider_key,
        date_key,
        shst_geometry_id,
        shst_reference_id,
        hash,
        datetime,
        vehicle_type,
        propulsion_type,
        bearing,
        speed,
        seen,
        batch
    from
        etl.external_shst_segment_hit as e
    inner join
        dim.provider as p on p.provider_id = e.provider_id
    where
        e.batch = '{{ ts_nodash }}'
    '''
)

parse_datalake_files_task >> [stage_shst_segment_hit_task, skip_warehouse_task]


delete_data_lake_extract_task = AzureDataLakeRemoveOperator(task_id=f'delete_extract',
                                                            dag=dag,
                                                            depends_on_past=False,
                                                            azure_data_lake_conn_id='azure_data_lake_default',
                                                            remote_path='/transportation/mobility/etl/shst_hits/{{ ts_nodash }}.csv')

stage_shst_segment_hit_task >> delete_data_lake_extract_task


warehouse_insert_task = MsSqlOperator(
    task_id='warehouse_insert_shst_segment_hits',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    pool='scooter_azure_sql_server',
    sql='''
    insert into
        fact.shst_segment_hit (
            provider_key,
            date_key,
            shst_geometry_id,
            shst_reference_id,
            hash,
            datetime,
            vehicle_type,
            propulsion_type,
            bearing,
            speed,
            first_seen,
            last_seen
        )
    select
        provider_key,
        date_key,
        shst_geometry_id,
        shst_reference_id,
        hash,
        datetime,
        vehicle_type,
        propulsion_type,
        bearing,
        speed,
        seen,
        seen
    from
        etl.stage_shst_segment_hit_{{ ts_nodash }} as source
    where not exists (
        select
            1
        from
            fact.shst_segment_hit as target
        where
         target.hash = source.hash
    )
    '''
)

warehouse_update_task = MsSqlOperator(
    task_id=f'warehouse_update_event',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    pool='scooter_azure_sql_server',
    sql='''
    update
        fact.shst_segment_hit
    set
        last_seen = source.seen,
        datetime = source.datetime,
        vehicle_type = source.vehicle_type,
        propulsion_type = source.propulsion_type,
        bearing = source.bearing,
        speed = source.speed
    from
        etl.stage_shst_segment_hit_{{ ts_nodash }} as source
    where
        source.hash = fact.shst_segment_hit.hash
    '''
)

stage_shst_segment_hit_task >> [warehouse_insert_task, warehouse_update_task]

clean_stage_task = MsSqlOperator(
    task_id='clean_stage_table',
    dag=dag,
    depends_on_past=False,
    mssql_conn_id='azure_sql_server_full',
    pool='scooter_azure_sql_server',
    sql='''
    if exists (
        select 1
        from sysobjects
        where name = 'stage_shst_segment_hit_{{ ts_nodash }}'
    )
    drop table etl.stage_shst_segment_hit_{{ ts_nodash }}
    '''
)

[warehouse_insert_task, warehouse_update_task] >> clean_stage_task
