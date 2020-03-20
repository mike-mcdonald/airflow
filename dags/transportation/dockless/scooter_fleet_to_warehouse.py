'''
DAG for ETL Processing of Dockless Mobility Provider Data
'''
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.mssql_plugin import MsSqlOperator
from airflow.operators.mobility_plugin import (
    MobilityFleetToSqlExtractOperator
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date':  datetime(2019, 4, 26),
    'email': ['michael.mcdonald@portlandoregon.gov'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 9,
    'retry_delay': timedelta(minutes=2),
    'concurrency': 1,
    'max_active_runs': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='scooter_fleet_to_warehouse',
    default_args=default_args,
    catchup=True,
    schedule_interval='@daily'
)

fleet_extract_task = MobilityFleetToSqlExtractOperator(
    task_id='build_fleet_extract',
    dag=dag,
    sql_conn_id='azure_sql_server_default',
    data_lake_conn_id='azure_data_lake_default',
    fleet_local_path='/usr/local/airflow/tmp/{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts_nodash }}.csv',
    fleet_remote_path='/transportation/mobility/etl/fleet_count/{{ ts_nodash }}.csv'
)

states_extract_task = MsSqlOperator(
    task_id='extract_active_states',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    if exists (
        select 1
        from sysobjects
        where name = 'extract_fleet_state_{{ ts_nodash }}'
    )
    drop table etl.extract_fleet_state_{{ ts_nodash }}

    create table etl.extract_fleet_state_{{ ts_nodash }}
    with
    (
        distribution = round_robin,
        clustered columnstore index
    )
    as
    select
        provider_key,
        vehicle_key,
        start_city_key as city_key,
        start_pattern_area_key as pattern_area_key,
        start_state as state,
        start_time,
        end_time
    from
        fact.state
    where
        start_time <= cast('{{ execution_date.in_timezone('America/Los_Angeles').strftime('%m/%d/%Y %H:%M:%S.%f') }}' as datetime2)
    and coalesce(
            end_time,
            cast('12/31/9999 23:59:59.9999' as datetime2)
        ) >= cast('{{ execution_date.in_timezone('America/Los_Angeles').subtract(days=2).strftime('%m/%d/%Y %H:%M:%S.%f') }}' as datetime2)
    ''')


clean_extract_after_task = MsSqlOperator(
    task_id=f'clean_extract_table_after',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    if exists (
        select 1
        from sysobjects
        where name = 'extract_fleet_state_{{ ts_nodash }}'
    )
    drop table etl.extract_fleet_state_{{ ts_nodash }}
    '''
)

fleet_stage_task = MsSqlOperator(
    task_id='stage_fleet_extract',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    if exists (
        select 1
        from sysobjects
        where name = 'stage_fleet_{{ ts_nodash }}'
    )
    drop table etl.stage_fleet_{{ ts_nodash }}

    create table etl.stage_fleet_{{ ts_nodash }}
    with (
        distribution = round_robin,
        clustered columnstore index
    )
    as
    select
        date_key,
        provider_key,
        city_key,
        pattern_area_key,
        time,
        coalesce(available, 0) as available,
        coalesce(reserved, 0) as reserved,
        coalesce(unavailable, 0) as unavailable,
        coalesce(removed, 0) as removed,
        seen,
        batch
    from
    (
        select
            e.date_key,
            e.provider_key,
            e.time,
            f.city_key,
            f.pattern_area_key,
            f.state,
            coalesce(f.count, 0) as count,
            seen,
            batch
        from
            etl.extract_fleet_count as e
        outer apply (
            select
                f.city_key as city_key,
                f.pattern_area_key as pattern_area_key,
                f.state,
                count(distinct f.vehicle_key) as count
            from
                etl.extract_fleet_state_{{ ts_nodash }} as f
            where
                f.provider_key = e.provider_key
                and f.start_time <= e.time
                and coalesce(
                    f.end_time,
                    cast('12/31/9999 23:59:59.9999' as datetime2)
                ) >= e.time
            group by
                f.city_key,
                f.pattern_area_key,
                f.state
        ) as f
        where
            e.batch = '{{ ts_nodash }}'
    ) p pivot (
        max(count) for state IN (
            [available],
            [reserved],
            [unavailable],
            [removed]
        )
    ) as pvt
    '''
)

fleet_extract_task >> fleet_stage_task

states_extract_task >> fleet_stage_task >> clean_extract_after_task

clean_stage_task_after = MsSqlOperator(
    task_id='clean_stage_table_after',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    if exists (
        select 1
        from sysobjects
        where name = 'stage_fleet_{{ ts_nodash }}'
    )
    drop table etl.stage_fleet_{{ ts_nodash }}
    '''
)


fleet_warehouse_update_task = MsSqlOperator(
    task_id='warehouse_update_fleet',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    update
        fact.fleet_count
    set
        available = source.available,
        reserved = source.reserved,
        unavailable = source.unavailable,
        removed = source.removed,
        last_seen = source.seen
    from
        etl.stage_fleet_{{ ts_nodash }} as source
    where
        source.provider_key = fact.fleet_count.provider_key
        and coalesce(source.city_key, -1) = coalesce(fact.fleet_count.city_key, -1)
        and coalesce(source.pattern_area_key, -1) = coalesce(fact.fleet_count.pattern_area_key, -1)
        and source.time = fact.fleet_count.time
    '''
)
fleet_warehouse_insert_task = MsSqlOperator(
    task_id='warehouse_insert_fleet',
    dag=dag,
    mssql_conn_id='azure_sql_server_full',
    sql='''
    insert
        fact.fleet_count (
            date_key,
            provider_key,
            city_key,
            pattern_area_key,
            time,
            available,
            reserved,
            unavailable,
            removed,
            first_seen,
            last_seen
        )
    select
        source.date_key,
        source.provider_key,
        source.city_key,
        source.pattern_area_key,
        source.time,
        source.available,
        source.reserved,
        source.unavailable,
        source.removed,
        source.seen,
        source.seen
    from
        etl.stage_fleet_{{ ts_nodash }} as source
    where not exists
    (
        select
            1
        from
            fact.fleet_count as target
        where
            source.provider_key = target.provider_key
            and coalesce(source.city_key, -1) = coalesce(target.city_key, -1)
            and coalesce(source.pattern_area_key, -1) = coalesce(target.pattern_area_key, -1)
            and source.time = target.time
    )
    '''
)

fleet_stage_task >> fleet_warehouse_insert_task >> clean_stage_task_after
fleet_stage_task >> fleet_warehouse_update_task >> clean_stage_task_after
