'''
DAG for ETL Processing of Waze feed objects
'''
import hashlib
import pathlib
import os

from datetime import datetime, timedelta

from pytz import timezone
from shapely.geometry import LineString, Point
from shapely.wkt import dumps

import airflow
from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.hooks.waze_plugin import WazeHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 8, 7),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'retries': 0,
    'concurrency': 3,
    'max_active_runs': 1,
    'depends_on_past': False,
}

dag = DAG(
    dag_id='waze_feeds_to_data_lake',
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


def alerts_to_data_lake(**kwargs):
    hook = WazeHook(waze_conn_id=kwargs['waze_conn_id'])

    alerts = hook.get_alerts()

    alerts = add_default_columns(
        kwargs, alerts, ['uuid', 'pubMillis'])

    alerts['pubMillis'] = alerts.pubMillis.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    alerts['pubMillis'] = alerts.pubMillis.dt.round('L')
    alerts['pubMillis'] = alerts.pubMillis.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
    alerts['pubMillis'] = alerts.pubMillis.map(lambda x: x[:-3])

    alerts['location'] = alerts.location.map(lambda x: [x['x'], x['y']])
    alerts['location'] = alerts.location.apply(Point)
    alerts['location'] = alerts.location.apply(dumps)

    pathlib.Path(os.path.dirname(kwargs['templates_dict']['local_path'])
                 ).mkdir(parents=True, exist_ok=True)

    alerts.to_csv(kwargs['templates_dict']['local_path'], index=False)

    hook = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['azure_data_lake_conn_id'])

    hook.upload_file(kwargs['templates_dict']['local_path'],
                     kwargs['templates_dict']['remote_path'])

    # os.remove(kwargs['templates_dict']['local_path'])


def jams_to_data_lake(**kwargs):
    hook = WazeHook(waze_conn_id=kwargs['waze_conn_id'])

    jams = hook.get_trafficjams()

    jams = add_default_columns(
        kwargs, jams, ['uuid', 'pubMillis'])

    jams['pubMillis'] = jams.pubMillis.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    jams['pubMillis'] = jams.pubMillis.dt.round('L')
    jams['pubMillis'] = jams.pubMillis.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
    jams['pubMillis'] = jams.pubMillis.map(lambda x: x[:-3])

    jams['line'] = jams.line.map(lambda l: [[x['x'], x['y']] for x in l])
    jams['line'] = jams.line.apply(LineString)
    jams['line'] = jams.line.apply(dumps)

    pathlib.Path(os.path.dirname(kwargs['templates_dict']['local_path'])
                 ).mkdir(parents=True, exist_ok=True)

    jams.to_csv(kwargs['templates_dict']['local_path'], index=False)

    hook = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['azure_data_lake_conn_id'])

    hook.upload_file(kwargs['templates_dict']['local_path'],
                     kwargs['templates_dict']['remote_path'])

    # os.remove(kwargs['templates_dict']['local_path'])


def irregularities_to_data_lake(**kwargs):
    hook = WazeHook(waze_conn_id=kwargs['waze_conn_id'])

    irregularities = hook.get_irregularities()

    irregularities = add_default_columns(
        kwargs, irregularities, ['id', 'detectionDateMillis'])

    del irregularities['detectionDate']
    del irregularities['updateDate']

    irregularities['detectionDateMillis'] = irregularities.detectionDateMillis.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    irregularities['detectionDateMillis'] = irregularities.detectionDateMillis.dt.round(
        'L')
    irregularities['detectionDateMillis'] = irregularities.detectionDateMillis.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
    irregularities['detectionDateMillis'] = irregularities.detectionDateMillis.map(
        lambda x: x[:-3])

    irregularities['updateDateMillis'] = irregularities.updateDateMillis.map(
        lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
    irregularities['updateDateMillis'] = irregularities.updateDateMillis.dt.round(
        'L')
    irregularities['updateDateMillis'] = irregularities.updateDateMillis.map(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
    irregularities['updateDateMillis'] = irregularities.updateDateMillis.map(
        lambda x: x[:-3])

    irregularities['line'] = irregularities.line.map(
        lambda l: [[x['x'], x['y']] for x in l])
    irregularities['line'] = irregularities.line.apply(LineString)
    irregularities['line'] = irregularities.line.apply(dumps)

    pathlib.Path(os.path.dirname(kwargs['templates_dict']['local_path'])).mkdir(
        parents=True, exist_ok=True)

    irregularities.to_csv(kwargs['templates_dict']['local_path'], index=False)

    hook = AzureDataLakeHook(
        azure_data_lake_conn_id=kwargs['azure_data_lake_conn_id'])

    hook.upload_file(kwargs['templates_dict']['local_path'],
                     kwargs['templates_dict']['remote_path'])
    # os.remove(kwargs['templates_dict']['local_path'])


alerts_to_data_lake_task = PythonOperator(
    task_id="waze_alerts_to_data_lake",
    dag=dag,
    python_callable=alerts_to_data_lake,
    provide_context=True,
    op_kwargs={
        'waze_conn_id': 'waze_portland',
        'azure_data_lake_conn_id': 'azure_data_lake_default',
    },
    templates_dict={
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/waze/etl/alert/raw/{{ ts_nodash }}.csv',
    }
)

jams_to_data_lake_task = PythonOperator(
    task_id="waze_jams_to_data_lake",
    dag=dag,
    python_callable=jams_to_data_lake,
    provide_context=True,
    op_kwargs={
        'waze_conn_id': 'waze_portland',
        'azure_data_lake_conn_id': 'azure_data_lake_default',
    },
    templates_dict={
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/waze/etl/jam/raw/{{ ts_nodash }}.csv',
    }
)

irregularities_to_data_lake_task = PythonOperator(
    task_id="waze_irregularities_to_data_lake",
    dag=dag,
    python_callable=irregularities_to_data_lake,
    provide_context=True,
    op_kwargs={
        'waze_conn_id': 'waze_portland',
        'azure_data_lake_conn_id': 'azure_data_lake_default',
    },
    templates_dict={
        'local_path': '/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
        'remote_path': '/transportation/waze/etl/irregularity/raw/{{ ts_nodash }}.csv',
    }
)
