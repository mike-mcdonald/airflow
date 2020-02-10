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
    dag_id='scooter_pilot_1_hits_to_warehouse',
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
            t.alternatekey,
            c.name as provider_name,
            v.key as vehicle_key,
            v.name as vehicle_id,
            sd.date as start_date,
            t.starttime as start_time,
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

    trips['trip_id'] = trips.key.map(lambda x: str(uuid.uuid4()))
    trips['geometry'] = trips.route.apply(lambda x: loads(x, hex=True))

    del trips['route']

    trips = gpd.GeoDataFrame(trips).set_geometry('geometry')
    trips.crs = 'epsg:4326'

    trips = trips.drop_duplicates(subset=['trip_id'])
    shst_series = trips[['trip_id', 'geometry']].apply(
        lambda x: {
            'type': 'FeatureCollection',
            'features': [{
                'type': 'Feature',
                'properties': {
                    'trip_id': x.trip_id
                },
                'geometry': {
                    'type': x.geometry.geom_type,
                    'coordinates': np.array(x.geometry).tolist()
                }
            }]
        }, axis=1)

    hook_shst = SharedStreetsAPIHook(shst_api_conn_id='shst_api_default')
    cores = cpu_count()  # Number of CPU cores on your system
    executor = ThreadPoolExecutor(max_workers=cores*4)
    shst_calls = shst_series.map(lambda x: executor.submit(
        hook_shst.match, 'line', 'bike', x))

    trips['batch'] = kwargs['templates_dict']['batch']
    trips['seen'] = datetime.now()
    trips['seen'] = trips.seen.dt.round('L')
    trips['seen'] = trips.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    trips['propulsion_type'] = 'electric,human'
    trips['vehicle_type'] = 'scooter'

    trips['datetime'] = trips.apply(
        lambda x: datetime.combine(x.start_date, datetime.min.time()) + x.start_time, axis=1)
    trips['datetime'] = trips.datetime.map(
        lambda x: datetime.replace(x, tzinfo=None))
    trips['datetime'] = trips.datetime.dt.round('H')
    trips['date_key'] = trips.datetime.map(
        lambda x: int(x.strftime('%Y%m%d')))
    trips['datetime'] = trips.datetime.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    def safe_result(x):
        try:
            return x.result()
        except:
            logging.error(
                'Error retrieving sharedstreets references, returning empty results...')
            return {'features': []}

    shst_df = shst_calls.map(safe_result)

    shst_df = pd.DataFrame({
        'feature': np.concatenate(shst_df.map(lambda x: x['features']).values)
    })

    shst_df['trip_id'] = shst_df.feature.map(
        lambda x: x['properties']['trip_id'])
    shst_df['candidate'] = shst_df.feature.map(
        lambda x: x['properties']['shstCandidate'])

    shst_df['confidence'] = shst_df.candidate.map(lambda x: x['confidence'])
    shst_df['segments'] = shst_df.candidate.map(lambda x: x['segments'])

    lens = [len(item) for item in shst_df['segments']]
    shst_df = pd.DataFrame({
        'trip_id': np.repeat(shst_df['trip_id'].values, lens),
        'confidence': np.repeat(shst_df['confidence'].values, lens),
        'candidate': np.concatenate(shst_df['segments'].values),
    })

    shst_df['shst_geometry_id'] = shst_df['candidate'].map(
        lambda x: x.get('geometryId'))
    shst_df['shst_reference_id'] = shst_df['candidate'].map(
        lambda x: x.get('referenceId'))

    trips = trips.merge(
        shst_df[['trip_id', 'shst_geometry_id', 'shst_reference_id', 'confidence']], on='trip_id')

    del trips['geometry']

    if len(trips) <= 0:
        logging.warning(
            f'Received no hits for time period {kwargs.get("execution_date")}')
        return f'warehouse_skipped'

    trips['hash'] = trips.apply(lambda x: hashlib.md5((
        f'{x.trip_id}{x.vehicle_id}{x.provider_name}{x.shst_reference_id}{os.environ["HASH_SALT"]}'
    ).encode('utf-8')).hexdigest(), axis=1)

    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    trips[[
        'provider_name',
        'date_key',
        'shst_geometry_id',
        'shst_reference_id',
        'confidence',
        'hash',
        'datetime',
        'vehicle_type',
        'propulsion_type',
        'seen',
        'batch'
    ]].to_csv(kwargs.get('templates_dict').get('local_path'), index=False)

    hook_data_lake = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs.get('azure_datalake_conn_id')
    )

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
    },
    op_kwargs={
        'azure_datalake_conn_id': 'azure_data_lake_default',
        'pgsql_conn_id': 'pgsql_scooter_pilot_1'
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
