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
from airflow.operators.mssql_plugin import MsSqlOperator

from airflow.hooks.zendesk_plugin import ZendeskHook
from airflow.hooks.azure_plugin import AzureDataLakeHook
from airflow.operators.azure_plugin import AzureDataLakeRemoveOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2020, 4, 4), #The task is triggered after start_date+interval has passed
    'email': ['abdullah.malikyar@portlandoregon.gov','Michael.McDonald@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=10),
}

custom_field_ids = {
    236316: 'Reason_For_Call_Request_Type',
    236307: 'Meter_ID_PK_Zone_APP_Zone',
    #236308: 'Meter Location',
    292134: 'Meter_Resolution',
    25205166: 'Paid_License_Plate',
    #25240326: 'Resolution/Conclusion Comments',
    360000118026: 'Citation_Date',
    360000119063: 'refund_type',
    360000118046: 'officer_no',
    360000077106: 'refund_request',
    360000034926: 'Customer_Service_Resolution',
    360000089246: 'Transaction_Date_and_Start_Time',
    360000089266: 'Transaction_Date_and_End_Time',
    #25191603: 'Citation #',
    360000139966: 'refund_amount',
    360000036566: 'Transaction_Permit_No',
    25338086: 'dismissal_request',
    360027931211: 'supervisor_call_back_request'
}


#setting remote and local paths
start_time_file_remote_path = '/zendesk/meta/next_start_time.csv'
start_time_file_local_path = 'usr/local/airflow/tmp/zendesk_meta/next_start_time.csv'
start_time_file_local_dir = 'usr/local/airflow/tmp/zendesk_meta'

def set_start_time(next_start_time, azure_dl_hook):
    '''
    update start time (epoch) in Data lake meta

    '''
    field = ['next_start_time']
    row = [next_start_time]

   #check if dirs exists if not create them.
    if not os.path.isdir(start_time_file_local_dir) :
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
    start_time = datetime(2016, 1, 1, 0, 0, 0, 0).timestamp()

    #check if last end time is stored for start time
    start_time_exists = azure_dl_hook.check_for_file(start_time_file_remote_path)

    logging.debug(f'start time remote file exist: {start_time_exists}')

    if start_time_exists :
        logging.debug(f'enterred the if block')
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

    az_hook = AzureDataLakeHook(
        azure_data_lake_conn_id = kwargs.get('zendesk_datalake_conn_id') 
    )

    start_time = get_start_time(azure_dl_hook=az_hook)
    
    logging.info(f'date time passed to url:{start_time}')

    tickets, next_start_time = hook.get_tickets(start_time=start_time)
    # Get tickets as data frams
    
    
    if len(tickets) <= 0:
        logging.warning(
            f'Received no tickets for time period {start_time} to {datetime.now()}')
        return 

    pathlib.Path(os.path.dirname(kwargs.get('templates_dict').get('tickets_local_path'))
                 ).mkdir(parents=True, exist_ok=True)

    def extract_column_ids_values(custom_fields):
        '''
        assigns custom field ID to its value, removing keywords id and value
        for example if a = { id: 'id', value: 1 }
        this will return id_value_dict which is { id: 1 }
        
        '''
        id_value_dict = {}
        ids = [x.get('id') for x in custom_fields]
        values = [x.get('value') for x in custom_fields]

        for i in ids:
            for x in values:
                id_value_dict[i] = x
                values.remove(x)
                break

        return id_value_dict

    tickets['custom_fields_1'] = tickets.custom_fields.apply(
    extract_column_ids_values)

    tickets = pd.concat([tickets.drop(['custom_fields_1'], axis=1), tickets['custom_fields_1'].apply(pd.Series)], axis=1)
    
    tickets = tickets.rename(columns=custom_field_ids)

    def clean_tags(x):
        '|'.join(x)
        y = [i.replace('"', '') for i in x]
        return y
    
    def remove_quotes(x):
        y = x
        if x != None:
            y = x.replace('"', '')
        return y

    tickets['tags_joined'] = tickets.tags.apply(clean_tags)
    tickets['batch'] = kwargs.get('templates_dict').get('batch')
    tickets['subject_clean'] = tickets.subject.apply(remove_quotes)
    tickets['reason_for_call_request_type_clean'] = tickets.Reason_For_Call_Request_Type.apply(remove_quotes)
    tickets['meter_id_pk_zone_app_zone_clean'] = tickets.Meter_ID_PK_Zone_APP_Zone.apply(remove_quotes)
    tickets['meter_resolution_clean'] = tickets.Meter_Resolution.apply(remove_quotes)
    tickets['paid_license_plate_clean'] = tickets.Paid_License_Plate.apply(remove_quotes)
    tickets['citation_date_clean'] = tickets.Citation_Date.apply(remove_quotes)
    tickets['officer_no_clean'] = tickets.officer_no.apply(remove_quotes)
    tickets['customer_service_resolution_clean'] = tickets.Customer_Service_Resolution.apply(remove_quotes)
    tickets['transaction_date_and_start_time_clean'] = tickets.Transaction_Date_and_Start_Time.apply(remove_quotes)
    tickets['transaction_date_and_end_time_clean'] = tickets.Transaction_Date_and_End_Time.apply(remove_quotes)
    tickets['transaction_permit_no_clean'] = tickets.Transaction_Permit_No.apply(remove_quotes)
    tickets['initially_assigned_at_date'] = tickets.dates.map(lambda x: x.get('initially_assigned_at'))
    tickets['solved_at_date'] = tickets.dates.map(lambda x: x.get('solved_at'))

    tickets[[
        'id',
        'created_at',
        'updated_at',
        'type',
        'subject_clean',
        'priority',
        'status',
        'assignee_id',
        'problem_id',
        'has_incidents',
        'tags_joined',
        'reason_for_call_request_type_clean',
        'meter_id_pk_zone_app_zone_clean',
        'meter_resolution_clean',
        'paid_license_plate_clean',
        'citation_date_clean',
        'refund_type',
        'officer_no_clean',
        'refund_request',
        'customer_service_resolution_clean',
        'transaction_date_and_start_time_clean',
        'transaction_date_and_end_time_clean',
        'refund_amount',
        'transaction_permit_no_clean',
        'dismissal_request',
        'supervisor_call_back_request',
        'initially_assigned_at_date',
        'solved_at_date',
        'batch'
    ]].to_csv(kwargs.get('templates_dict').get('tickets_local_path'), index=False)

    az_hook.upload_file(kwargs.get('templates_dict').get(
        'tickets_local_path'), kwargs.get('templates_dict').get('tickets_remote_path'))
    os.remove(kwargs.get('templates_dict').get('tickets_local_path'))

    #save start_time for next execution
    set_start_time(next_start_time=next_start_time, azure_dl_hook=az_hook)

# retrieves zendesk ticket data and stores it to ADLS
dag_id = 'tickets_to_ADW'
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    schedule_interval='@weekly'
)
zendesk_conn_id = 'Zendesk_API'

tickets_remote_path = f'/zendesk/tickets/zendesk-tickets-{{{{ ts_nodash }}}}.csv'


retrieve_zendesk_data_task = PythonOperator(
    task_id='retrieve_zendesk_data',
    dag=dag,
    depends_on_past=False,
    pool='zendesk_api_pool',
    provide_context=True,
    python_callable=zendesk_tickets_to_datalake,
    op_kwargs={
        'zendesk_api_conn_id': zendesk_conn_id,
        'zendesk_datalake_conn_id':'azure_data_lake_default'
    },
    templates_dict={
        'batch': f'{{{{ ts_nodash }}}}',
        'tickets_local_path': f'usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/zendesk-tickets-{{{{ ts_nodash }}}}.csv',
        'tickets_remote_path': tickets_remote_path,
    }
)

# delete previous record of updated tickets 
delete_updated_tickets = MsSqlOperator(
    task_id='delete_updated_tickets',
    dag=dag,
    depends_on_past=False,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    delete
    from
        zendesk.zendesk_ticket
    where
        id in ( 
            select 
                id 
            from
                etl.external_zendesk_ticket
            where 
                batch = '{{ ts_nodash }}'
        )
    '''
)

retrieve_zendesk_data_task >> delete_updated_tickets

# Process new batch of tickets 
insert_new_batch = MsSqlOperator(
    task_id='insert_new_batch_of_tickets',
    dag=dag,
    depends_on_past=False,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    insert into
        zendesk.zendesk_ticket
    select
        *
    from
        etl.external_zendesk_ticket
    where
        batch = '{{ts_nodash}}'
    '''
)

delete_updated_tickets >> insert_new_batch

delete_processed_batches_ADLS = AzureDataLakeRemoveOperator(
    task_id=f'delete_processed_batches_ADLS',
    dag=dag,
    depends_on_past=False,
    provide_context=True,
    azure_data_lake_conn_id='azure_data_lake_default',
    remote_path = tickets_remote_path)

insert_new_batch >> delete_processed_batches_ADLS

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