"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityTripsToSqlExtractOperator,
    MobilityTripsToSqlWarehouseOperator,
    MobilityProviderSyncOperator,
    MobilityVehicleSyncOperator
)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date":  datetime(2019, 4, 26),
    "email": ["pbotsqldbas@portlandoregon.gov"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 9,
    "retry_delay": timedelta(minutes=2),
    "concurrency": 1,
    "max_active_runs": 1,
    # "queue": "bash_queue",
    # "pool": "backfill",
    # "priority_weight": 10,
    # "end_date": datetime(2016, 1, 1),
}

dag = DAG(
    dag_id="scooter_trips_to_warehouse",
    default_args=default_args,
    catchup=True,
    schedule_interval="@hourly",
)

providers = ["lime", "spin", "bolt"]

task1 = DummyOperator(
    task_id="provider_extract_start",
    dag=dag
)

task2 = DummyOperator(
    task_id="provider_extract_complete",
    dag=dag
)

clean_extract_task = MsSqlOperator(
    task_id="clean_extract_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.extract_trip WHERE batch = '{{ ts_nodash }}'
    DELETE FROM etl.extract_segment_hit WHERE batch = '{{ ts_nodash }}'
    """
)
clean_extract_task.set_downstream(task1)

# Extract data from providers and stage in tables
for provider in providers:
    mobility_provider_conn_id = f"mobility_provider_{provider}"
    mobility_provider_token_conn_id = f"mobility_provider_{provider}_token"

    trip_extract_task = MobilityTripsToSqlExtractOperator(
        task_id=f"loading_{provider}_trips",
        provide_context=True,
        mobility_provider_conn_id=mobility_provider_conn_id,
        mobility_provider_token_conn_id=mobility_provider_token_conn_id,
        sql_conn_id="azure_sql_server_default",
        dag=dag)

    trip_extract_task.set_upstream(task1)
    trip_extract_task.set_downstream(task2)


clean_stage_task = MsSqlOperator(
    task_id="clean_stage_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_trip WHERE batch = '{{ ts_nodash }}'
    DELETE FROM etl.stage_segment_hit WHERE batch = '{{ ts_nodash }}'
    """
)

task2 >> clean_stage_task

provider_sync_task = MobilityProviderSyncOperator(
    task_id="provider_sync",
    source_table="etl.extract_trip",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)

task2 >> provider_sync_task

vehicle_sync_task = MobilityVehicleSyncOperator(
    task_id="vehicle_sync",
    source_table="etl.extract_trip",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)

task2 >> vehicle_sync_task


trip_load_task.set_downstream(task3)
