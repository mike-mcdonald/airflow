'''
DAG for ETL Processing of Waze feed objects
'''
import hashlib
import pathlib
import os

from datetime import datetime, timedelta

from pytz import timezone
from shapely.geometry import LineString, Point
from shapely.wkt import dumps, loads

import airflow
from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.dataframe_plugin import MsSqlDataFrameHook
from airflow.operators.mssql_plugin import MsSqlOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 8, 7),
    'email': ['michael.mcdonald@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'retries': 0,
    'concurrency': 3,
    'max_active_runs': 1,
    'depends_on_past': False,
}

dag = DAG(
    dag_id='waze_jams_geoevents_raw_sql_to_warehoused_sql',
    catchup=False,
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)


def add_default_columns(context, dataframe, hash_columns):
    def hash(jam):
        cols = [col for col in jam if col in hash_columns]
        s = ''
        for col in cols:
            s = s + str(jam.get(col))

        return hashlib.md5(s.encode('utf-8')).hexdigest()

    dataframe['hash'] = dataframe.apply(hash, axis=1)

    dataframe['batch'] = context.get('ts_nodash')

    dataframe['seen'] = datetime.now().astimezone(timezone('US/Pacific'))
    dataframe['seen'] = dataframe.seen.dt.round('L')
    dataframe['seen'] = dataframe.seen.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
    dataframe['seen'] = dataframe.seen.map(lambda x: x[:-3])

    return dataframe


def extract_jams(**kwargs):
    hook = MsSqlDataFrameHook(
        mssql_conn_id=kwargs['sql_server_conn_id']
    )

    df = hook.read_sql_dataframe(sql='''
    select
        uuid,
        pubMillis,
        level,
        length,
        delay,
        speed,
        time_added_to_db as seen,
        Shape as geometry
    from
        pdot.wazetrafficjam as e
    ''')

    df['hash'] = df.apply(lambda x: hashlib.md5(
        '/'.join([x.uuid, x.pubMillis]).encode('utf-8')), axis=1)

    df['batch'] = kwargs.get('ts_nodash')

    # process dataframe groups
    grouped = df.groupby(by='hash')

    df[['start_time', 'min_level', 'min_speed', 'min_delay',
        'min_length']] = grouped[['pubMillis', 'level', 'speed', 'delay',
                                  'length']].transform('min')
    df[['end_time', 'max_level', 'max_speed', 'max_delay',
        'max_length']] = grouped[['seen', 'level', 'speed', 'delay',
                                  'length']].transform('max')
    df[['avg_level', 'avg_speed', 'avg_delay',
        'avg_length']] = grouped[['level', 'speed', 'delay',
                                  'length']].transform('mean')
    df['times_seen'] = grouped.count()['uuid']

    hook.write_dataframe(df[['hash', 'start_time', 'end_time', 'min_level', 'min_speed', 'min_delay',
                             'min_length', 'max_level', 'max_speed', 'max_delay',
                             'max_length', 'avg_level', 'avg_speed', 'avg_delay',
                             'avg_length', 'times_seen', 'batch', 'seen']],
                         'extract_waze_jam',
                         schema='pbot',
                         if_exists='append',
                         index=False,
                         index_label=None,
                         chunksize=None,
                         dtype=None,
                         method=None)


jams_extract_task = PythonOperator(
    task_id="stage_jams_sql",
    dag=dag,
    python_callable=extract_jams,
    provide_context=True,
    op_kwargs={
        'sql_server_conn_id': 'local_sql_waze',
        'azure_data_lake_conn_id': 'azure_data_lake_default',
    },
    templates_dict={
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/waze/etl/jam/raw/{{ ts_nodash }}.csv',
    }
)

# clean_extract_task = MsSqlOperator()

# clean_stage_before_task = MsSqlOperator()

# clean_stage_after_task = MsSqlOperator()

# jams_stage_task = MsSqlOperator()
