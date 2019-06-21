"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityEventsToSqlExtractOperator,
    MobilityEventsToSqlStageOperator,
    MobilityStatesToSqlWarehouseOperator,
    MobilityProviderSyncOperator,
    MobilityVehicleSyncOperator,
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
    "max_active_runs": 1
    # "queue": "bash_queue",
    # "pool": "backfill",
    # "priority_weight": 10,
    # "end_date": datetime(2016, 1, 1),
}

dag = DAG(
    dag_id="scooter_events_to_warehouse",
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

# Extract data from providers and stage in tables
for provider in providers:
    mobility_provider_conn_id = f"mobility_provider_{provider}"
    mobility_provider_token_conn_id = f"mobility_provider_{provider}_token"

    event_extract_task = MobilityEventsToSqlExtractOperator(
        task_id=f"loading_{provider}_events",
        provide_context=True,
        mobility_provider_conn_id=mobility_provider_conn_id,
        mobility_provider_token_conn_id=mobility_provider_token_conn_id,
        sql_conn_id="azure_sql_server_default",
        data_lake_conn_id="azure_data_lake_default",
        events_local_path=f"/usr/local/airflow/tmp/{{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{provider}-{{{{ ts_nodash }}}}.csv",
        events_remote_path=f"/transportation/mobility/etl/event/{provider}-{{{{ ts_nodash }}}}.csv",
        dag=dag)

    event_extract_task.set_upstream(task1)
    event_extract_task.set_downstream(task2)

# Run SQL scripts to transform extract data into staged facts
event_stage_task = MobilityEventsToSqlStageOperator(
    task_id=f"staging_states",
    provide_context=True,
    mssql_conn_id="azure_sql_server_full",
    dag=dag)

provider_sync_task = MobilityProviderSyncOperator(
    task_id="provider_sync",
    source_table="etl.extract_event",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)
provider_sync_task.set_upstream(task2)
provider_sync_task.set_downstream(event_stage_task)

vehicle_sync_task = MobilityVehicleSyncOperator(
    task_id="vehicle_sync",
    source_table="etl.extract_event",
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)
vehicle_sync_task.set_upstream(task2)
vehicle_sync_task.set_downstream(event_stage_task)

clean_stage_task = MsSqlOperator(
    task_id="clean_stage_table",
    dag=dag,
    mssql_conn_id="azure_sql_server_full",
    sql="""
    DELETE FROM etl.stage_state WHERE batch = '{{ ts_nodash }}'
    """
)
clean_stage_task.set_upstream(task2)
clean_stage_task.set_downstream(event_stage_task)

task3 = DummyOperator(
    task_id="provider_staging_complete",
    dag=dag
)

event_stage_task.set_downstream(task3)

mobility_states_warehouse_task = MobilityStatesToSqlWarehouseOperator(
    task_id=f"warehousing_states",
    provide_context=True,
    mssql_conn_id="azure_sql_server_full",
    dag=dag
)
mobility_states_warehouse_task.set_upstream(task3)
