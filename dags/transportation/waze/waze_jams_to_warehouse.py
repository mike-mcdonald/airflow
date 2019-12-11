'''
DAG for ETL Processing of Waze alerts
'''
import json
import logging
import os
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import cpu_count, Pool

import numpy as np
import pandas as pd
import pytz
from requests import Session
from shapely.wkt import loads

import airflow
from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.hooks.azure_plugin import AzureDataLakeHook

SHAREDSTREETS_API_URL = 'http://sharedstreets:3000/api/v1/match/line/car'

default_args = {
    'owner': 'airflow',
    'start_date':  datetime(2019, 8, 7),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(minutes=2),
    'concurrency': 1,
    'max_active_runs': 1
}

dag = DAG(
    dag_id='waze_jams_to_warehouse',
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily'
)


def process_datalake_files(**kwargs):
    remote_path = '/transportation/waze/etl/jam/raw'

    hook = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['azure_data_lake_conn_id'])

    files = hook.ls(remote_path)

    df = []

    for file in files:
        # create local_path
        head, tail = os.path.split(file)
        local_path = f'/usr/local/airflow/tmp/waze/jam/raw-{tail}'
        pathlib.Path(os.path.dirname(local_path)
                     ).mkdir(parents=True, exist_ok=True)
        # download file
        hook.download_file(local_path, file)
        df.append(pd.read_csv(local_path))
        os.remove(local_path)

    df = pd.concat(df, sort=False).sort_values(by=['hash'])

    shst_df = df.groupby(by='hash').apply(lambda x: {
        'type': 'FeatureCollection',
        'features': x.apply(lambda x: {
            'type': 'Feature',
            'properties': {
                'hash': x.hash,
                'batch': x.batch
            },
            'geometry': {
                'type': 'LineString',
                'coordinates': list(loads(x.line).coords)
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
                    logging.warning(
                        f'Unable to retrieve response from {url} after 3 tries.  Aborting...')
                    return res

                logging.warning(
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
            return {'features': []}

    shst_df = pd.DataFrame({
        'feature': np.concatenate(shst.map(safe_result).map(lambda x: x.get('features')).values)
    })

    logging.debug('Retrieved features from sharedstreets...')

    shst_df['hash'] = shst_df.feature.map(lambda x: x['properties']['hash'])
    shst_df['batch'] = shst_df.feature.map(lambda x: x['properties']['batch'])
    shst_df['segments'] = shst_df.feature.map(
        lambda x: x['properties']['shstCandidate']['segments'])

    # Pivot on segments
    lens = [len(item) for item in shst_df['segments']]
    shst_df = pd.DataFrame({
        "hash": np.repeat(shst_df['hash'].values, lens),
        "batch": np.repeat(shst_df['batch'].values, lens),
        "segment": np.concatenate(shst_df['segments'].values),
    })
    shst_df['shst_geometry_id'] = shst_df.segment.map(
        lambda x: x.get('geometryId'))
    shst_df['shst_reference_id'] = shst_df.segment.map(
        lambda x: x.get('referenceId'))

    df = df.merge(shst_df, on=['hash', 'batch'],
                  how='left').sort_values(by='hash')

    df['hash'] = df.apply(
        lambda x: hashlib.md5(f'{x.uuid}{x.pubMillis}{x.shst_geometry_id}{x.shst_reference_id}'.encode('utf-8')).hexdigest())

    del df['line']

    logging.debug('Merged sharedstreets information...')

    grouped = df.groupby(['hash'])

    df[[
        'start_time',
        'min_level',
        'min_speed',
        'min_delay',
        'min_length'
    ]] = df.merge(
        grouped.min()[['pubMillis', 'level', 'speed', 'delay', 'length']],
        on='hash',
        how='left',
        suffixes=('_orig', '_min')
    )[[
      'pubMillis_min',
      'level_min',
      'speed_min',
      'delay_min',
      'length_min'
      ]]

    df[[
        'end_time',
        'max_level',
        'max_speed',
        'max_delay',
        'max_length'
    ]] = df.merge(
        grouped.max()[['seen', 'level', 'speed', 'delay', 'length']],
        on='hash',
        how='left',
        suffixes=('_orig', '_max')
    )[[
      'seen_max',
      'level_max',
      'speed_max',
      'delay_max',
      'length_max'
      ]]

    df[[
        'avg_level',
        'avg_speed',
        'avg_delay',
        'avg_length'
    ]] = df.merge(
        grouped.mean()[['level', 'speed', 'delay', 'length']],
        on='hash',
        how='left',
        suffixes=('_orig', '_avg')
    )[[
        'level_avg',
        'speed_avg',
        'delay_avg',
        'length_avg'
    ]]

    df[['times_seen']] = df.merge(
        grouped.count()['uuid'],
        on='hash',
        how='left',
        suffixes=('_orig', '_count')
    )[['uuid_count']]

    df = df.drop_duplicates(subset=['hash'])

    logging.debug('Writing grouped output to data lake...')

    # write processed output
    local_path = f'/usr/local/airflow/tmp/waze/jam/processed-{kwargs["ts_nodash"]}.csv'
    df[[
        'shst_geometry_id',
        'shst_reference_id',
        'hash',
        'uuid',
        'start_time',
        'min_level',
        'min_speed',
        'min_delay',
        'min_length',
        'end_time',
        'max_level',
        'max_speed',
        'max_delay',
        'max_length',
        'avg_level',
        'avg_speed',
        'avg_delay',
        'avg_length',
        'times_seen',
        'batch',
        'seen'
    ]].to_csv(local_path, index=False)

    remote_path = f'/transportation/waze/etl/jam/processed/{kwargs["ts_nodash"]}.csv'
    hook.upload_file(local_path, remote_path)
    os.remove(local_path)

    for file in files:
        hook.rm(file)


parse_datalake_files_task = PythonOperator(
    task_id='parse_datalake_files',
    dag=dag,
    provide_context=True,
    python_callable=process_datalake_files,
    op_kwargs={'azure_data_lake_conn_id': 'azure_data_lake_default'}
)
