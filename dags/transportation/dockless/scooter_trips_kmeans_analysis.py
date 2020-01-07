'''
DAG for ETL Processing of PDX GIS Open Data Counties, from Metro
'''
import logging
import pathlib
import os
import pickle

from datetime import datetime, timedelta

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from shapely.geometry import box, Point
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
    dag_id='scooter_kmeans_update',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    max_active_runs=3,
)

providers = ['lime', 'spin', 'bolt', 'shared', 'razor', 'bird']


def update_kmeans_object(**kwargs):
    hook = MsSqlDataFrameHook(mssql_conn_id=kwargs['sql_server_conn_id'])

    trip_counts = hook.read_sql_dataframe(sql=f'''
    select
        start_date_key,
        count(trip_id) as trips
    from
        fact.trip
    where
        start_time between '{kwargs['execution_date'].in_timezone('America/Los_Angeles').subtract(months=6).strftime('%m/%d/%Y') }' and '{kwargs['execution_date'].in_timezone('America/Los_Angeles').strftime('%m/%d/%Y')}'
    group by
        start_date_key
    order by
        trips desc
    ''')

    trip_counts = trip_counts.sort_values(by=['trips'])
    max_date_key = trip_counts.loc[0].start_date_key

    start_time = datetime.strptime(str(max_date_key), '%Y%m%d').replace(
        tzinfo=timezone('US/Pacific')).astimezone(timezone('UTC'))
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
        lambda x: x['features'])

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
    route_df['geometry'] = route_df.feature.map(
        lambda x: Point(x['geometry']['coordinates'])
    )

    route_df = gpd.GeoDataFrame(route_df)

    # Eliminate points that are outside the bounds of the metro area cities
    hook = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs.get('azure_data_lake_conn_id'))
    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('cities_local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    cities = hook.download_file(
        kwargs.get('templates_dict').get('cities_local_path'),
        kwargs.get('templates_dict').get('cities_remote_path'))
    cities = gpd.read_file(kwargs.get(
        'templates_dict').get('cities_local_path'))
    hull = cities.geometry.map(lambda x: box(
        x.bounds[0], x.bounds[1], x.bounds[2], x.bounds[3])).unary_union.convex_hull
    route_df = route_df[route_df.geometry.within(hull)]

    del cities
    del hull

    # convert coordinates into a multi-dimensional array for kmeans
    coord_array = []
    route_df.coordinates.map(lambda x: coord_array.append([x[0], x[1]]))
    coord_array = np.array(coord_array)

    # Update kmeans object
    kmeans = KMeans(n_clusters=50)
    kmeans.fit(coord_array)

    with open(kwargs.get('templates_dict').get('local_path')) as f:
        pickle.dump(kmeans, f)

    hook.upload_file(kwargs.get('templates_dict').get(
        'local_path'), kwargs.get('templates_dict').get('remote_path'))


parse_datalake_files_task = PythonOperator(
    task_id='update_kmeans',
    dag=dag,
    provide_context=True,
    python_callable=update_kmeans_object,
    templates_dict={
        'cities_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/cities-{{ ts_nodash }}.csv',
        'cities_remote_path': '/transportation/mobility/dim/cities.csv',
        'remote_path': '/transportation/mobility/learning/kmeans.pkl',
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/kmeans-{{ ts_nodash }}.pkl',
    },
    op_kwargs={
        'sql_server_conn_id': 'azure_sql_server_full',
        'azure_data_lake_conn_id': 'azure_data_lake_default'
    },
)
