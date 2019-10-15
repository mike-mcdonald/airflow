'''
DAG for ETL Processing of Waze alerts
'''
import os
from datetime import datetime, timedelta

import pandas as pd
import pytz

import airflow
from airflow import DAG

from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.azure_plugin import AzureDataLakeHook

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
    dag_id='waze_trafficjams_to_warehouse',
    catchup=False,
    default_args=default_args,
    schedule_interval='@hourly'
)


def process_datalake_files(azure_datalake_conn_id, **kwargs):
    remote_path = '/transportation/waze/etl/traffic_jam/raw/'

    hook = AzureDataLakeHook(azure_datalake_conn_id=azure_datalake_conn_id)

    files = hook.ls(remote_path)

    df = []

    for file in files:
        # create local_path
        local_path = f'/usr/local/airflow/tmp/waze/traffic_jam/raw/{file}'
        pathlib.Path(os.path.dirname(kwargs['templates_dict']['local_path'])
                     ).mkdir(parents=True, exist_ok=True)
        # download file
        hook.download_file(local_path, f'{remote_path}/{file}')
        df = df.append(pd.read_csv(local_path))
        os.remove(local_path)

    df = pd.concat(df, sort=False).sort_values(by=['hash'])

    # process dataframe
    grouped = df.groupby(by='hash')

    df[['start_time', 'min_level', 'min_speed', 'min_delay',
        'min_length']] = grouped.min()[['pubMillis', 'level', 'speed', 'delay',
                                        'length']]
    df[['end_time', 'max_level', 'max_speed', 'max_delay',
        'max_length']] = grouped.max()[['seen', 'level', 'speed', 'delay',
                                        'length']]
    df[['avg_level', 'avg_speed', 'avg_delay',
        'avg_length']] = grouped.mean()[['level', 'speed', 'delay',
                                         'length']]
    df['times_seen'] = grouped.count()['uuid']

    # write processed output
    local_path = f'/usr/local/airflow/tmp/waze/traffic_jam/{kwargs['execution_date']}.csv'
    df.to_csv(local_path)
    remote_path = f'/transportation/waze/etl/traffic_jam/processed/{kwargs['execution_date']}.csv'
    hook.upload_file(local_path, remote_path)
    os.remove(local_path)

    for file in files:
        # delete file
        hook.rm(f'{remote_path}/{file}')


parse_datalake_files_task = PythonOperator(
    task_id='parse_datalake_files',
    dag=dag,
    provide_context=True,
    python_callable=process_datalake_files,
    op_kwargs={'azure_datalake_conn_id': 'azure_datalake_default'},
    templates_dict
)


event_external_stage_task = MsSqlOperator(
    task_id='extract_external_batch',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    insert into
        etl.extract_trafficjam (
            [hash],
            [uuid],
            [pubMillis]
        )
    select
        [hash],
        [uuid],
        [start_time]
    from
        etl.external_event
    where
        batch = '{{ ts_nodash }}'
    '''
)
