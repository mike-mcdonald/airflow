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
from sklearn.cluster import KMeans

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.dataframe_plugin import MsSqlDataFrameHook
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


def update_kmeans_object(**kwargs):
    hook = MsSqlDataFrameHook()

    trip_counts = hook.read_sql_dataframe(sql='''
    select
        start_date_key,
        count(trip_id) as trips
    from
        fact.trip
    ''')

    trip_counts = trip_counts.sort_values(by=['trips'])
    max_date_key = trip_counts.loc[0].start_date_key

    start_time = datetime.strptime(max_date_key, '%y%m%d')
    end_time = start_time + timedelta(hours=24)

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
        logging.error(
            f'Received no trips for time period {start_time} to {end_time}')
        raise Error

    trips['route'] = trips.route.apply(
        lambda x: x['features'], axis=1)

    lens = [len(item) for item in trips['route']]

    route_df = pd.DataFrame({
        "trip_id": np.repeat(trips['trip_id'].values, lens),
        "feature": np.concatenate(trips['route'].values)
    })

    del trips

    route_df['timestamp'] = route_df.feature.map(
        lambda x: x['properties']['timestamp'])
    route_df['coordinates'] = route_df.feature.map(
        lambda x: x['geometry']['coordinates']
    )
    route_df['geometry'] = route_df.coordinates.apply(Point)

    # convert coordinates into a multi-dimensional array for kmeans
    coord_array = []
    route_df.coordinates.map(lambda x: coord_array.append([x[0], x[1]]))
    coord_array = np.array(coord_array)

    kmeans = KMeans(n_clusters=50)
    kmeans.fit(coord_array)

    pickle.dump(kmeans, kwargs.get('templates_dict').get('local_path'))

    hook.upload_file(kwargs.get('templates_dict').get(
        'local_path'), kwargs.get('templates_dict').get('remote_path'))


parse_datalake_files_task = PythonOperator(
    task_id='update_kmeans',
    dag=dag,
    provide_context=True,
    python_callable=update_kmeans_object,
    templates_dict={
        'remote_path': '/transportation/mobility/etl/learning/kmeans.pkl',
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/kmeans-{{ ts_nodash }}.pkl',
    },
    op_kwargs={
        'sql_server_conn_id': 'sql_server_default'
    },
)
