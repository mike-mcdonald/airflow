'''
DAG for ETL Processing of PDX GIS Open Data Counties, from Metro
'''
import hashlib
import json
import logging
import pathlib
import os
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
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.mobility_plugin import MobilityProviderHook

SHAREDSTREETS_API_URL = 'http://sharedstreets:3000/api/v1/match/point/bike'

default_args = {
    'owner': 'airflow',
    'start_date':  datetime(2019, 4, 26),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
}

dag = DAG(
    dag_id='scooter_shst_hits_to_data_lake',
    default_args=default_args,
    catchup=True,
    schedule_interval='@hourly',
    max_active_runs=3,
)

providers = ['lime', 'spin', 'bolt', 'shared', 'razor', 'bird']


def extract_shst_hits_datalake(**kwargs):
    end_time = kwargs.get('execution_date') - timedelta(hours=24)
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
        print(
            f'Received no trips for time period {start_time} to {end_time}')
        return

    # Convert the route to a DataFrame now to make mapping easier
    trips['route'] = trips.apply(
        lambda x: x.route['features'] if x.shst is not None else x.route['features'], axis=1)

    lens = [len(item) for item in trips['route']]

    route_df = pd.DataFrame({
        "trip_id": np.repeat(trips['trip_id'].values, lens),
        "provider_id": np.repeat(trips['provider_id'].values, lens),
        "device_id": np.repeat(trips['device_id'].values, lens),
        "vehicle_type": np.repeat(trips['vehicle_type'].values, lens),
        "propulsion_type": np.repeat(trips['propulsion_type'].values, lens),
        "feature": np.concatenate(trips['route'].values)
    })

    del trips

    route_df['timestamp'] = route_df.feature.map(
        lambda x: x['properties']['timestamp'])
    route_df['coordinates'] = route_df.feature.map(
        lambda x: x['geometry']['coordinates']
    )
    route_df['geometry'] = route_df.coordinates.apply(Point)

    route_df = gpd.GeoDataFrame(route_df.sort_values(
        by=['trip_id', 'timestamp'], ascending=True
    ).reset_index(drop=True).copy())
    route_df.crs = {'init': 'epsg:4326'}
    route_df['datetime'] = route_df.timestamp.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    route_df['datetime'] = route_df.datetime.dt.round('L')
    route_df['datetime'] = route_df.datetime.map(
        lambda x: datetime.replace(x, tzinfo=None))
    route_df['date_key'] = route_df.datetime.map(
        lambda x: int(x.strftime('%Y%m%d')))
    # Generate a hash to aid in merge operations
    route_df['hash'] = route_df.apply(lambda x: hashlib.md5((
        f'{x.trip_id}{x.device_id}{x.provider_id}{x.timestamp}{os.environ['HASH_SALT']}'
    ).encode('utf-8')).hexdigest(), axis=1)
    route_df['datetime'] = route_df.datetime.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    hook = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs.get('azure_datalake_conn_id'))

    hook.download_file('/transportation/mobility/learning/kmeans.pkl',
                       kwargs['templates_dict']['kmeans_local_path'])

    kmeans = pickle.load(kwargs['templates_dict']['kmeans_local_path'])

    # convert coordinates into a multi-dimensional array for kmeans
    coord_array = []
    route_df.coordinates.map(lambda x: coord_array.append([x[0], x[1]]))
    coord_array = np.array(coord_array)

    route_df['cluster'] = kmeans.predict(coord_array)

    shst_df = route_df.groupby('cluster').apply(lambda x: {
        'type': 'FeatureCollection',
        'features': x.apply(lambda x: {
            'type': 'Feature',
            'properties': {
                'hash': x.hash
            },
            'geometry': {
                'type': 'Point',
                'coordinates': [x.coordinates[0], x.coordinates[1]]
            }
        }, axis=1).values.tolist()
    })

    def _request(session, url, data=None):
        '''
        Internal helper for sending requests.

        Returns payload(s).
        '''
        retries = 0
        res = None

        while res is None:
            try:
                res = session.post(url, data=data)
                res.raise_for_status()
            except Exception as err:
                res = None
                retries = retries + 1
                if retries > 3:
                    print(
                        f'Unable to retrieve response from {url} after 3 tries.  Aborting...')
                    return res

                print(
                    f'Error while retrieving: {err}. Retrying in 10 seconds... (retry {retries}/3)')
                time.sleep(10)

        return res

    session = Session()

    session.headers.update({'Content-Type': 'application/json'})
    session.headers.update({'Accept': 'application/json'})

    cores = cpu_count()  # Number of CPU cores on your system
    executor = ThreadPoolExecutor(max_workers=cores*4)
    shst = shst_df.map(lambda x: executor.submit(
        _request, session, SHAREDSTREETS_API_URL, data=json.dumps(x)))

    def safe_result(x):
        try:
            return x.result().json()
        except:
            return None

    trips['propulsion_type'] = trips.propulsion_type.map(
        lambda x: ','.join(sorted(x)))

    trips['shst'] = shst.map(safe_result)

    route_df = route_df.to_crs(epsg=3857)

    route_df['x'] = route_df.geometry.map(lambda g: g.x)
    route_df['y'] = route_df.geometry.map(lambda g: g.y)

    route_by_trip = route_df.groupby(['trip_id'])

    route_df['nt'] = route_by_trip.timestamp.shift(-1)
    route_df['nx'] = route_by_trip.x.shift(-1)
    route_df['ny'] = route_by_trip.y.shift(-1)

    # drop destination
    route_df = route_df.dropna().copy()

    route_df['dx'] = route_df.apply(
        lambda x: x.nx - x.x, axis=1)
    route_df['dy'] = route_df.apply(
        lambda x: x.ny - x.y, axis=1)
    route_df['dt'] = route_df.apply(
        lambda x: (x.nt - x.timestamp) / 1000, axis=1)

    def find_bearing(hit):
        deg = atan2(hit.dx, hit.dy) / pi * 180
        if deg < 0:
            deg = deg + 360
        return deg

    def find_speed(hit):
        if hit['dt'] <= 0:
            return 0

        d = sqrt(pow((hit.dx), 2) + pow((hit.dy), 2))

        return d / hit['dt']

    route_df['bearing'] = route_df.apply(find_bearing, axis=1)
    route_df['speed'] = route_df.apply(find_speed, axis=1)

    shst_df = pd.DataFrame({
        'feature': np.concatenate(shst.map(safe_result).map(lambda x: x['features']).values)
    })
    shst_df['hash'] = shst_df.feature.map(lambda x: x['properties']['hash'])
    shst_df['candidates'] = shst_df.feature.map(
        lambda x: x['properties']['shstCandidates'])

    route_df = route_df.merge(shst_df, on='hash')

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

    shst_df['bearing_diff'] = shst_df.apply(lambda x: fabs(
        normalizeAngle(x.bearing) - normalizeAngle(x.shstBearing)), axis=1)

    shst_df['batch'] = kwargs.get('ts_nodash')
    shst_df['seen'] = datetime.now()
    shst_df['seen'] = shst_df.seen.dt.round('L')
    shst_df['seen'] = shst_df.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    shst_df.sort_values(
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

    hook.upload_file(kwargs.get('templates_dict').get(
        'local_path'), kwargs.get('templates_dict').get('remote_path'))


parse_datalake_files_task = PythonOperator(
    task_id='process_segments_to_csv_files',
    dag=dag,
    provide_context=True,
    python_callable=extract_shst_hits_datalake,
    templates_dict={
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/mobility/etl/shst_hits/{{ ts_nodash }}.csv',
        'kmeans_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/kmeans-{{ ts_nodash }}.pkl',
    },
    op_kwargs={
        'azure_datalake_conn_id': 'azure_data_lake_default'
    },
)
