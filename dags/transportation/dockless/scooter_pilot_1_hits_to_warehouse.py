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
from shapely.wkb import loads

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.hooks.common_plugin import PgSqlDataFrameHook
from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.dataframe_plugin import AzureMsSqlDataFrameHook
from airflow.hooks.mobility_plugin import SharedStreetsAPIHook

from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator
from airflow.operators.mssql_plugin import MsSqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2018, 4, 26),
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
    end_time = kwargs['execution_date']
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
        return f'warehouse_skipped'

    trips['geometry'] = trips.route.apply(lambda x: loads(x, hex=True))

    del trips['route']

    trips = gpd.GeoDataFrame(trips).set_geometry('geometry')
    trips.crs = 'epsg:4326'

    if len(trips) <= 0:
        logging.warning(
            f'Received no trips for time period {start_time} to {end_time}')
        return f'warehouse_skipped'

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

    trips['datetime'] = trips.apply(lambda x: x.start_date + x.start_time)
    trips['datetime'] = trips.datetime.map(
        lambda x: datetime.replace(x, tzinfo=None))
    trips['datetime'] = trips.datetime.dt.round('H')
    trips['date_key'] = trips.datetime.map(
        lambda x: int(x.strftime('%Y%m%d')))
    trips['datetime'] = trips.datetime.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    def safe_result(x):
        try:
            return x.result().json()
        except:
            logging.error(
                'Error retrieving sharedstreets references, returning empty results...')
            return {'features': []}

    shst_df = shst_calls.map(safe_result)

    shst_df = pd.DataFrame({
        'trip_id': shst.map(lambda x: x['features'][0]['properties']['trip_id']),
        'candidate': shst.map(lambda x: x['features'][0]['properties']['shstCandidate'])
    })

    shst_df = shst_df[~shst_df.candidate.isnull()]

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
        shst_df[['trip_id', 'geometry_id', 'reference_id', 'confidence']], on='trip_id')

    del trips['geometry']

    trips['hash'] = trips.apply(lambda x: hashlib.md5((
        f'{x.trip_id}{x.vehicle_id}{x.provider_name}{x.reference_id}{os.environ["HASH_SALT"]}'
    ).encode('utf-8')).hexdigest(), axis=1)

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

    hook_datalake.upload_file(kwargs.get('templates_dict').get(
        'local_path'), kwargs.get('templates_dict').get('remote_path'))

    os.remove(kwargs.get('templates_dict').get('local_path'))

    return 'stage_shst_segment_hits'


extract_data_lake_task = BranchPythonOperator(
    task_id='extract_routes_to_data_lake',
    dag=dag,
    provide_context=True,
    python_callable=extract_shst_hits_datalake,
    templates_dict={
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/mobility/etl/pilot_1/shst_hits/{{ ts_nodash }}.csv',
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
        seen,
        batch
    from
        etl.external_shst_segment_hit as e
    inner join
        dim.provider as p on p.provider_name = e.provider_name
    where
        e.batch = '{{ ts_nodash }}'
    '''
)

extract_data_lake_task >> [sync_vehicle_task, skip_warehouse_task]

stage_shst_segment_hit_task


delete_data_lake_extract_task = AzureDataLakeRemoveOperator(
    task_id=f'delete_extract',
    dag=dag,
    depends_on_past=False,
    azure_data_lake_conn_id='azure_data_lake_default',
    remote_path='/transportation/mobility/etl/shst_hits/{{ ts_nodash }}.csv'
)

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
