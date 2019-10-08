"""
DAG for ETL Processing of PDX GIS Open Data Counties, from Metro
"""
import hashlib
import json
import logging
import pathlib
import os
import time

from datetime import datetime, timedelta
from multiprocessing import cpu_count, Pool

import geopandas as gpd

from requests import Session
from shapely.wkt import dumps

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mobility_plugin import MobilityHook

SHAREDSTREETS_API_URL = 'http://sharedstreets:3000/api/v1/match/point/bike'

default_args = {
    "owner": "airflow",
    "start_date":  datetime(2019, 4, 26),
    "email": ["pbotsqldbas@portlandoregon.gov"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 9,
    "retry_delay": timedelta(minutes=1),
    "concurrency": 1,
    "max_active_runs": 1
}

dag = DAG(
    dag_id="shst_segments_to_data_lake",
    default_args=default_args,
    schedule_interval=None
)

providers = ["lime", "spin", "bolt", "shared", "razor", "bird"]


def extract_shst_hits_datalake(**kwargs):
    end_time = kwargs.get('execution_date') - timedelta(hours=24)
    start_time = end_time - timedelta(hours=1)

    trips = []
    for provider in providers:
        hook = MobilityHook(
            mobility_provider_conn_id=f'mobility_provider_{provider}',
            mobility_provider_token_conn_id=f'mobility_provider_{provider}_token'
        )

        trips.extend(hook.get_trips(
            min_end_time=start_time, max_end_time=end_time))

    # Get trips as a GeoDataFrame
    trips = gpd.GeoDataFrame(trips)
    trips.crs = {'init': 'epsg:4326'}

    if len(trips) <= 0:
        self.log.warning(
            f"Received no trips for time period {start_time} to {end_time}")
        return

    def _request(session, url, data=None):
        """
        Internal helper for sending requests.

        Returns payload(s).
        """
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
                        f"Unable to retrieve response from {url} after 3 tries.  Aborting...")

                print(
                    f"Error while retrieving: {err}. Retrying in 10 seconds... (retry {retries}/3)")
                time.sleep(10)

        return res

    session.headers.update({"Content-Type": "application/json"})
    session.headers.update({"Accept": "application/json"})

    cores = cpu_count()  # Number of CPU cores on your system
    executor = ThreadPoolExecutor(max_workers=cores*4)
    shst = trips.route.map(lambda x: executor.submit(
        _request, session, 'http://sharedstreets:3000/api/v1/match/point/bike', data=json.dumps(x)))

    def safe_result(x):
        try:
            return x.result().json()
        except:
            return None

    trips['shst'] = shst.map(safe_result)

    # Convert the route to a DataFrame now to make mapping easier
    trips['route'] = trips.apply(
        lambda x: gpd.GeoDataFrame.from_features(x.shst['features']) if x.shst is not None else gpd.GeoDataFrame.from_features(x.route['features']), axis=1)

    def parse_route(trip):
        route = trip.route
        route['trip_id'] = trip.trip_id
        route['provider_id'] = trip.provider_id
        route['vehicle_id'] = trip.vehicle_id
        route['device_id'] = trip.device_id
        route['vehicle_type'] = trip.vehicle_type
        route['propulstion_type'] = list(trip.propulsion_type)
        return route

    route_df = gpd.GeoDataFrame(
        pd.concat(df.apply(parse_route, axis=1).values, sort=False).sort_values(
            by=['trip_id', 'timestamp'], ascending=True
        )
    ).reset_index(drop=True)
    route_df.crs = {'init': 'epsg:4326'}
    route_df['datetime'] = route_df.timestamp.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
    route_df['datetime'] = route_df.datetime.dt.round("L")
    route_df['datetime'] = route_df.datetime.map(
        lambda x: datetime.replace(x, tzinfo=None))
    route_df['date_key'] = route_df.datetime.map(
        lambda x: int(x.strftime('%Y%m%d')))
    # Generate a hash to aid in merge operations
    route_df['hash'] = route_df.apply(lambda x: hashlib.md5((
        x.trip_id + x.device_id + x.provider_id + x.timestamp
    ).encode('utf-8')).hexdigest(), axis=1)
    route_df['datetime'] = route_df.datetime.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    route_df = route_df.to_crs(epsg=3857)

    route_df['x'] = route_df.geometry.map(lambda g: g.x)
    route_df['y'] = route_df.geometry.map(lambda g: g.y)

    route_by_trip = route_df.groupby(['trip_id'])

    route_df['nt'] = route_by_trip.timestamp.shift(-1)
    route_df['nx'] = route_by_trip.x.shift(-1)
    route_df['ny'] = route_by_trip.y.shift(-1)

    # drop destination
    route_df = route_df.dropna()

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
    route_df['shstCandidates'] = route_df.shstCandidates.map(
        lambda x: pd.DataFrame(x)
    )

    def expand_candidates(p):
        df = p.shstCandidates.rename(
            index=str, columns={'bearing': 'shstBearing'})
        df['hash'] = p.hash
        df['trip_id'] = p.trip_id
        df['timestamp'] = p.timestamp
        df['vehicle_type'] = p.vehicle_type
        df['bearing'] = p.bearing
        df['speed'] = p.speed

        return df

    shst_df = pd.concat(route_df.apply(expand_candidates, axis=1).values, sort=False).sort_values(
        by=['trip_id', 'timestamp'], ascending=True
    )

    def normalizeAngle(angle):
        if angle < 0:
            angle = angle + 360
        return angle

    shst_df['bearing_diff'] = shst_df.apply(lambda x: fabs(
        normalizeAngle(x.bearing) - normalizeAngle(x.shstBearing)), axis=1)

    shst_df['batch'] = kwargs.get("ts_nodash")
    shst_df['seen'] = datetime.now()
    shst_df['seen'] = shst_df.seen.dt.round("L")
    shst_df['seen'] = shst_df.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    shst_df.sort_values(
        by=['hash', 'bearing_diff', 'score']
    ).drop_duplicates(
        subset=['hash']
    ).drop_duplicates(
        subset=['trip_id', 'geometryId'],
        keep='last'
    ).rename(
        index=str,
        columns={
            'geometryId': 'shst_geometry_id',
            'referenceId': 'shst_reference_id'
        }
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
    ]].to_csv(kwargs.get('local_path'))

    # hook = AzureDataLakeHook(
    #     azure_data_lake_conn_id=kwargs.get('azure_data_lake_conn_id'))

    # hook.upload_file(kwargs.get('local_path'), kwargs.get('remote_path'))

    # os.remove(kwargs.get('local_path'))


parse_datalake_files_task = PythonOperator(
    task_id='process_segments_to_geojson_files',
    dag=dag,
    provide_context=True,
    python_callable=process_segments_geojson,
    op_kwargs={
        'azure_datalake_conn_id': 'azure_datalake_default',
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/mobility/etl/shst_hits/{{ ts_nodash }}.csv'
    },
)
