from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityStatesToSqlWarehouseOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 * args, **kwargs):
        sql = """
        UPDATE fact.state
        SET last_seen = source.seen,
        end_hash = source.end_hash,
        end_date_key = source.end_date_key,
        end_state = source.end_state,
        end_event = source.end_event,
        end_time = source.end_time,
        end_location = source.end_location,
        end_battery_pct = source.end_battery_pct,
        duration = source.duration
        FROM etl.stage_state AS source
        WHERE source.start_hash = fact.state.start_hash
        AND source.batch = '{{ ts_nodash }}'

        INSERT fact.state
        (
            provider_key
            ,vehicle_key
            ,propulsion_type
            ,start_hash
            ,start_date_key
            ,start_state
            ,start_event
            ,start_time
            ,start_location
            ,start_battery_pct
            ,end_hash
            ,end_date_key
            ,end_state
            ,end_event
            ,end_time
            ,end_location
            ,end_battery_pct
            ,associated_trip
            ,duration
            ,first_seen
            ,last_seen
        )
        SELECT source.provider_key
        ,source.vehicle_key
        ,source.propulsion_type
        ,source.start_hash
        ,source.start_date_key
        ,source.start_state
        ,source.start_event
        ,source.start_time
        ,source.start_location
        ,source.start_battery_pct
        ,source.end_hash
        ,source.end_date_key
        ,source.end_state
        ,source.end_event
        ,source.end_time
        ,source.end_location
        ,source.end_battery_pct
        ,source.associated_trip
        ,source.duration
        ,source.seen
        ,source.seen
        FROM etl.stage_state AS source
        WHERE source.batch = '{{ ts_nodash }}'
        AND NOT EXISTS
        (
            SELECT 1
            FROM fact.state AS target
            WHERE target.start_hash = source.start_hash
        )
        """
        super().__init__(sql, *args, **kwargs)
