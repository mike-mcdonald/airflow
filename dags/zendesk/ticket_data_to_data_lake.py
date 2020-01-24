'''
DAG for ETL Processing of Zendesk Ticket Data
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
from shapely.wkt import loads

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.hooks.zendesk_plugin import ZendeskAzureDLHook , ZendeskHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2019, 12, 12),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(seconds=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def zendesk_tickets_to_datalake(**kwargs):

    # set initial start time if it is the first time
    # otherwise get the end time of last request and pass it as a start time for this request.
    start_time = datetime(2020, 1, 22).timestamp()

    # Create the hook
    hook = ZendeskHook(
        zendesk_conn_id=kwargs.get('zendesk_api_conn_id'))
    
    
    logging.info(f'-------------------date time passed to url:{start_time}')
    # Get tickets as data frams
    tickets = gpd.GeoDataFrame(hook.get_tickets(start_time=start_time))
    if len(tickets) <= 0:
        logging.warning(
            f'Received no tickets for time period {start_time} to {datetime.now()}')
        return f'warehouse_skipped'
    
    
    # do data transformation on tickets as needed here.
    hook = ZendeskAzureDLHook(
        zendesk_datalake_conn_id=kwargs.get('zendesk_datalake_conn_id') # maybe different in my case
    )

    #revise.
    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('tickets_local_path'))
                 ).mkdir(parents=True, exist_ok=True)
    
    tickets[[
        'id',
        'external_id',
        'created_at',
        'updated_at',
        'type',
        'subject',
        'raw_subject',
        'description',
        'priority',
        'status',
        'recipient',
        'group_id',
        'forum_topic_id',
        'problem_id',
        'has_incidents',
        'is_public',
        'due_at',
        'tags',
    ]].to_csv(kwargs.get('templates_dict').get('tickets_local_path'), index=False)

    hook.upload_file(kwargs.get('templates_dict').get(
        'tickets_local_path'), kwargs.get('templates_dict').get('tickets_remote_path'))
    os.remove(kwargs.get('templates_dict').get('tickets_local_path'))



dag_id = "ticket_data_to_data_lake"
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    catchup=True,
    schedule_interval='@monthly'
)
zendesk_conn_id = "Zendesk_API"

tickets_remote_path = f'/zendesk/tickets/zendesk-tickets-{{{{ ts_nodash }}}}.csv'


retrieve_zendesk_data_task = BranchPythonOperator(
    task_id=f'retrieve_zendesk_data',
    dag=dag,
    depends_on_past=False,
    pool='zendesk_api_pool',
    provide_context=True,
    python_callable=zendesk_tickets_to_datalake,
    op_kwargs={
        'zendesk_api_conn_id': zendesk_conn_id,
        'zendesk_datalake_conn_id':'azure_data_lake_zendesk'
    },
    templates_dict={
        'batch': f'{{{{ ts_nodash }}}}',
        'tickets_local_path': f'usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/zendesk-tickets-{{{{ ts_nodash }}}}.csv',
        'tickets_remote_path': tickets_remote_path,
    }
)


'''warehouse_skipped = DummyOperator(
    task_id=f'warehouse_skipped',
    dag=dag,
    depends_on_past=False,
)'''

start = DummyOperator(
    task_id=f'start',
    dag=dag,
    depends_on_past=False,
)



globals()[dag_id] = dag  #why?

start >> retrieve_zendesk_data_task 