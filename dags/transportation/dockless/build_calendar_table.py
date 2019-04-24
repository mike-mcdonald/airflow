"""
DAG for Creating a calendar table
"""

from datetime import datetime, timedelta

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.calendar_plugin import CalendarToSqlTableOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': None,
    'start_date':  datetime(2019, 4, 22),
    'email': ['pbotsqldbas@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'concurrency': 50
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='build-calendar-table',
    default_args=default_args
)

task = CalendarToSqlTableOperator(
    task_id="calendar_table_to_sql",
    azure_sql_conn_id="azure_sql_server_default",
    start=datetime(2000, 1, 1),
    end=datetime(2040, 12, 31),
    dag=dag
)
