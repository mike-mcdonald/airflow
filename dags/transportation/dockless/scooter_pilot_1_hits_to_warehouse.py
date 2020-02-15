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
from math import atan2, fabs, pi, pow, sqrt
from multiprocessing import cpu_count
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
from airflow.hooks.dataframe_plugin import AzureMsSqlDataFrameHook
from airflow.hooks.dataframe_plugin import PgSqlDataFrameHook
from airflow.hooks.mobility_plugin import SharedStreetsAPIHook

from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator
from airflow.operators.mssql_plugin import MsSqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2018, 4, 26),
    'end_date': datetime(2018, 12, 31),
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

dag = DAG(
    dag_id='scooter_pilot_1_segment_hits_to_warehouse',
    default_args=default_args,
    catchup=True,
    schedule_interval='@daily',
    max_active_runs=3,
)


def extract_shst_hits_to_data_lake(**kwargs):
    # Create the hook
    hook_pgsql = PgSqlDataFrameHook(
        pgsql_conn_id=kwargs.get('pgsql_conn_id')
    )

    # Get trips as a GeoDataFrame
    trips = hook_pgsql.read_sql_dataframe(f'''
        select
            t.key,
            c.name as provider_name,
            v.key as vehicle_key,
            v.name as vehicle_id,
            sd.date as start_date,
            t.starttime as start_time,
            ed.date as end_date,
            t.endtime as end_time,
            t.route
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
            sd.date = '{kwargs.get("execution_date").strftime("%Y-%m-%d")}'
        and
            t.route is not null
        and
            t.citystartkey is not null
        and
            t.cityendkey is not null
    ''')

    if len(trips) <= 0:
        logging.warning(
            f'Received no trips for {kwargs.get("execution_date")}')
        return f'warehouse_skipped'

    hook_data_lake = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['data_lake_conn_id']
    )

    pathlib.Path(os.path.dirname(kwargs.get('template_dict').get('trip_ids_local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    df = hook_data_lake.download_file(
        kwargs.get('template_dict').get('trip_ids_local_path'),
        kwargs.get('template_dict').get('trip_ids_remote_path')
    )
    df = gpd.read_file(kwargs.get('template_dict').get('trip_ids_local_path'))

    os.remove(kwargs.get('template_dict').get('trip_ids_local_path'))

    trip['trip_id'] = trip.merge(df, on='key')['trip_id']

    trips['geometry'] = trips.route.apply(lambda x: loads(x, hex=True))

    del trips['route']

    trips = gpd.GeoDataFrame(trips).set_geometry('geometry')
    trips.crs = 'epsg:4326'

    trips['batch'] = kwargs['templates_dict']['batch']
    trips['seen'] = datetime.now()
    trips['seen'] = trips.seen.dt.round('L')
    trips['seen'] = trips.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    trips['propulsion_type'] = 'electric,human'
    trips['vehicle_type'] = 'scooter'

    trips['start_time'] = trips.apply(
        lambda x: datetime.combine(x.start_date, datetime.min.time()) + x.start_time, axis=1)
    trips['start_time'] = trips.start_time.map(
        lambda x: datetime.replace(x, tzinfo=None))

    trips['end_time'] = trips.apply(
        lambda x: datetime.combine(x.end_date, datetime.min.time()) + x.end_time, axis=1)
    trips['end_time'] = trips.end_time.map(
        lambda x: datetime.replace(x, tzinfo=None))

    trips['timespan'] = trips.end_time - trips.start_time

    trips['coordinates'] = trips.geometry.map(lambda x: x.xy)

    trips['x'] = trips.coordinates.map(lambda x: x[0])
    trips['y'] = trips.coordinates.map(lambda x: x[1])

    trips['num_points'] = trips.geometry.map(lambda x: len(x.coords))
    trips['time_chunk'] = trips.timespan / trips.num_points

    lens = [len(item) for item in trips['x']]

    route_df = pd.DataFrame({
        'key': np.repeat(trips['key'].values, lens),
        'provider_name': np.repeat(trips['provider_name'].values, lens),
        'vehicle_key': np.repeat(trips['vehicle_key'].values, lens),
        'vehicle_id': np.repeat(trips['vehicle_id'].values, lens),
        'start_time': np.repeat(trips['start_time'].values, lens),
        'time_chunk': np.repeat(trips['time_chunk'].values, lens),
        'x': np.concatenate(trips['x'].values),
        'y': np.concatenate(trips['y'].values)
    })

    route_df['geometry'] = route_df.apply(lambda x: Point(x.x, x.y), axis=1)

    route_df = gpd.GeoDataFrame(
        route_df,
        crs={'init': 'epsg:4326'}
    )

    route_df['row_num'] = route_df.groupby('key').cumcount()
    route_df['datetime'] = route_df.start_time + \
        (route_df.time_chunk * route_df.row_num)

    route_df['hash'] = route_df.apply(lambda x: hashlib.md5((
        f"{x.key}{x.provider_name}{x.datetime.strftime('%d%m%Y%H%M%S%f')}"
    ).encode('utf-8')).hexdigest(), axis=1)

    route_df = route_df.to_crs(epsg=3857)

    route_df['x'] = route_df.geometry.map(lambda g: g.x)
    route_df['y'] = route_df.geometry.map(lambda g: g.y)

    route_by_trip = route_df.groupby(['key'])

    route_df['nt'] = route_by_trip.datetime.shift(-1)
    route_df['nx'] = route_by_trip.x.shift(-1)
    route_df['ny'] = route_by_trip.y.shift(-1)

    # drop destination
    route_df = route_df.dropna().copy()

    route_df['dx'] = route_df.nx - route_df.x
    route_df['dy'] = route_df.ny - route_df.y
    route_df['dt'] = (route_df.nt - route_df.datetime).map(lambda x: x.seconds)

    def find_bearing(hit):
        return atan2(hit.dx, hit.dy) / pi * 180

    def find_speed(hit):
        if hit['dt'] <= 0:
            return 0

        d = sqrt(pow((hit.dx), 2) + pow((hit.dy), 2))

        return d / hit['dt']

    route_df['heading'] = route_df.apply(find_bearing, axis=1)
    route_df['speed'] = route_df.apply(find_speed, axis=1)

    route_df = route_df.to_crs(epsg=4326)

    hook_mssql = AzureMsSqlDataFrameHook(
        azure_mssql_conn_id=kwargs.get('mssql_conn_id')
    )

    # Break out segment hits
    segments = hook_mssql.read_table_dataframe(
        table_name="segment", schema="dim")
    segments['geometry'] = segments.wkt.map(lambda g: loads(g))
    segments = gpd.GeoDataFrame(segments)
    segments.crs = {'init': 'epsg:4326'}

    route_df['segment_key'] = gpd.sjoin(
        route_df, segments, how="left", op="within"
    )[['hash_left', 'key']].drop_duplicates(subset='hash_left')['key']

    route_df = route_df.drop_duplicates(
        subset=['segment_key', 'trip_id'], keep='last')

    route_df['datetime'] = route_df.datetime.dt.round('H')
    route_df['date_key'] = route_df.datetime.map(
        lambda x: int(x.strftime('%Y%m%d')))
    route_df['datetime'] = route_df.datetime.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    route_df[[
        'provider_name',
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
    ]].to_csv(kwargs.get('templates_dict').get('local_path'), index=False)

    hook_data_lake.upload_file(kwargs.get('templates_dict').get(
        'local_path'), kwargs.get('templates_dict').get('remote_path'))

    os.remove(kwargs.get('templates_dict').get('local_path'))

    return 'stage_shst_segment_hits'


extract_data_lake_task = BranchPythonOperator(
    task_id='extract_routes_to_data_lake',
    dag=dag,
    provide_context=True,
    python_callable=extract_shst_hits_to_data_lake,
    templates_dict={
        'batch': 'pilot_1_{{ ts_nodash }}',
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/mobility/etl/pilot_1/shst_hits/{{ ts_nodash }}.csv',
        'trip_ids_local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/trip_ids-{{ ts_nodash }}.csv',
        'trip_ids_remote_path': '/transportation/mobility/etl/pilot_1/trip_ids.csv',
    },
    op_kwargs={
        'azure_datalake_conn_id': 'azure_data_lake_default',
        'pgsql_conn_id': 'pgsql_scooter_pilot_1',
        'mssql_conn_id': 'azure_sql_server_default'
    },


)

# skip_warehouse_task = DummyOperator(
#     task_id=f'warehouse_skipped',
#     dag=dag,
#     depends_on_past=False,
# )

# stage_shst_segment_hit_task = MsSqlOperator(
#     task_id=f'stage_shst_segment_hits',
#     dag=dag,
#     mssql_conn_id='azure_sql_server_full',
#     pool='scooter_azure_sql_server',
#     sql='''
#     if exists (
#         select 1
#         from sysobjects
#         where name = 'stage_shst_segment_hit_{{ ts_nodash }}'
#     )
#     drop table etl.stage_shst_segment_hit_{{ ts_nodash }}

#     create table etl.stage_shst_segment_hit_{{ ts_nodash }}
#     with
#     (
#         distribution = round_robin,
#         heap
#     )
#     as
#     select
#         p.[key] as provider_key,
#         date_key,
#         shst_geometry_id,
#         shst_reference_id,
#         confidence,
#         hash,
#         datetime,
#         vehicle_type,
#         propulsion_type,
#         seen,
#         batch
#     from
#         etl.external_pilot_1_shst_segment_hit as e
#     inner join
#         dim.provider as p on p.provider_name = e.provider_name
#     where
#         e.batch = '{{ ts_nodash }}'
#     '''
# )

# extract_data_lake_task >> [stage_shst_segment_hit_task, skip_warehouse_task]

# delete_data_lake_extract_task = AzureDataLakeRemoveOperator(
#     task_id=f'delete_extract',
#     dag=dag,
#     depends_on_past=False,
#     azure_data_lake_conn_id='azure_data_lake_default',
#     remote_path='/transportation/mobility/etl/pilot_1/shst_hits/{{ ts_nodash }}.csv'
# )

# stage_shst_segment_hit_task >> delete_data_lake_extract_task

# warehouse_insert_task = MsSqlOperator(
#     task_id='warehouse_insert_shst_segment_hits',
#     dag=dag,
#     mssql_conn_id='azure_sql_server_full',
#     pool='scooter_azure_sql_server',
#     sql='''
#     insert into
#         fact.shst_segment_hit (
#             provider_key,
#             date_key,
#             shst_geometry_id,
#             shst_reference_id,
#             confidence,
#             hash,
#             datetime,
#             vehicle_type,
#             propulsion_type,
#             first_seen,
#             last_seen
#         )
#     select
#         provider_key,
#         date_key,
#         shst_geometry_id,
#         shst_reference_id,
#         confidence,
#         hash,
#         datetime,
#         vehicle_type,
#         propulsion_type,
#         seen,
#         seen
#     from
#         etl.stage_shst_segment_hit_{{ ts_nodash }} as source
#     where not exists (
#         select
#             1
#         from
#             fact.shst_segment_hit as target
#         where
#             target.hash = source.hash
#     )
#     '''
# )

# warehouse_update_task = MsSqlOperator(
#     task_id=f'warehouse_update_event',
#     dag=dag,
#     mssql_conn_id='azure_sql_server_full',
#     pool='scooter_azure_sql_server',
#     sql='''
#     update
#         fact.shst_segment_hit
#     set
#         last_seen = source.seen,
#         datetime = source.datetime,
#         vehicle_type = source.vehicle_type,
#         propulsion_type = source.propulsion_type,
#     from
#         etl.stage_shst_segment_hit_{{ ts_nodash }} as source
#     where
#         source.hash = fact.shst_segment_hit.hash
#     '''
# )

# stage_shst_segment_hit_task >> [warehouse_insert_task, warehouse_update_task]

# clean_stage_task = MsSqlOperator(
#     task_id='clean_stage_table',
#     dag=dag,
#     depends_on_past=False,
#     mssql_conn_id='azure_sql_server_full',
#     pool='scooter_azure_sql_server',
#     sql='''
#     if exists (
#         select 1
#         from sysobjects
#         where name = 'stage_shst_segment_hit_{{ ts_nodash }}'
#     )
#     drop table etl.stage_shst_segment_hit_{{ ts_nodash }}
#     '''
# )

# [warehouse_insert_task, warehouse_update_task] >> clean_stage_task
