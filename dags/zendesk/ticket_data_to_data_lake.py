'''
DAG for ETL Processing of Zendesk Ticket Data
'''
import hashlib
import json
import logging
import pathlib
import os
import csv

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
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.zendesk_plugin import ZendeskAzureDLHook , ZendeskHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2020, 1, 20),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(seconds=10),
}


#setting remote and local paths
start_time_file_remote_path = f'/zendesk/meta/next_start_time.csv'
start_time_file_local_path = f'usr/local/airflow/tmp/zendesk_meta/next_start_time.csv'
start_time_file_local_dir = f'usr/local/airflow/tmp/zendesk_meta'

'''
update start time (epoch) in Data lake meta
'''

def set_start_time(next_start_time, azure_dl_hook):
    field = ['next_start_time']
    row = [next_start_time]

   #check if dirs exists if not create them.
    if os.path.isdir(start_time_file_local_dir) == False:
       os.makedirs(start_time_file_local_dir, exist_ok=True)

    if os.path.exists(start_time_file_local_path):
        os.remove(start_time_file_local_path)

    if os.path.isdir(start_time_file_local_dir):
        with open(start_time_file_local_path, 'a') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(field)
            writer.writerow(row)

    if os.path.exists(start_time_file_local_path):
        azure_dl_hook.upload_file(start_time_file_local_path, start_time_file_remote_path, overwrite=True)



'''
    returns start time for API call
'''
def get_start_time(azure_dl_hook):
    #initial start time
    start_time = datetime(2019, 1, 1, 9, 0, 0, 0).timestamp()

    #check if last end time is stored for start time
    start_time_exist = azure_dl_hook.check_for_file(start_time_file_remote_path)

    logging.info(f'-------------------- start time remote file exist: {start_time_exist}')

    if start_time_exist == True:

        logging.info(f'-------------------- enterred the if block')
        #download the file and read its value and return it.
        azure_dl_hook.download_file(start_time_file_local_path, start_time_file_remote_path, overwrite=True)
        
        #check if file was downloaded
        if(os.path.exists(start_time_file_local_path)):
            with open(start_time_file_local_path) as start_time_csv:
                csv_reader = csv.reader(start_time_csv, delimiter=',')
                for row in csv_reader:
                    start_time = str(row[0])

    return start_time


def zendesk_tickets_to_datalake(**kwargs):

    # Create hooks
    hook = ZendeskHook(
        zendesk_conn_id=kwargs.get('zendesk_api_conn_id'))

    az_hook = ZendeskAzureDLHook(
        zendesk_datalake_conn_id=kwargs.get('zendesk_datalake_conn_id') 
    )

    start_time = get_start_time(azure_dl_hook=az_hook)
    
    logging.info(f'-------------------date time passed to url:{start_time}')

    tickets, next_start_time = hook.get_tickets(start_time=start_time)
    # Get tickets as data frams
    
    
    if len(tickets) <= 0:
        logging.warning(
            f'Received no tickets for time period {start_time} to {datetime.now()}')
        return 
    
    
    # do data transformation on tickets as needed here.

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

    az_hook.upload_file(kwargs.get('templates_dict').get(
        'tickets_local_path'), kwargs.get('templates_dict').get('tickets_remote_path'))
    os.remove(kwargs.get('templates_dict').get('tickets_local_path'))

    #save start_time for next execution
    set_start_time(next_start_time=next_start_time, azure_dl_hook=az_hook)


dag_id = "ticket_data_to_data_lake"
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    schedule_interval='@hourly'
)
zendesk_conn_id = "Zendesk_API"

tickets_remote_path = f'/zendesk/tickets/zendesk-tickets-{{{{ ts_nodash }}}}.csv'


retrieve_zendesk_data_task = PythonOperator(
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


# '''warehouse_skipped = DummyOperator(
#     task_id=f'warehouse_skipped',
#     dag=dag,
#     depends_on_past=False,
# )'''

# start = DummyOperator(
#     task_id=f'start',
#     dag=dag,
#     depends_on_past=False,
# )


# start >> retrieve_zendesk_data_task 