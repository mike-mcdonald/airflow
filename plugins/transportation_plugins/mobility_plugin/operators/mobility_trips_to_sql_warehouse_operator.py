from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityTripsToSqlWarehouseOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 *args, **kwargs):
        sql = """
        UPDATE fact.trip
        SET last_seen = source.seen
        FROM etl.extract_trip AS source 
        WHERE source.trip_id = fact.state.trip_id
        AND source.batch = '{{ ts_nodash }}'

        INSERT (
            provider_key
            ,vehicle_key
            ,propulsion_type
            ,trip_id
            ,duration
            ,distance
            ,accuracy
            ,start_time
            ,end_time
            ,parking_verification_url
            ,standard_cost
            ,actual_cost
            ,origin
            ,destination
            ,first_seen
            ,last_seen
        )  
        SELECT
        source.provider_key
        ,source.vehicle_key
        ,source.propulsion_type
        ,source.trip_id
        ,source.duration
        ,source.distance
        ,source.accuracy
        ,source.start_time
        ,source.end_time
        ,source.parking_verification_url
        ,source.standard_cost
        ,source.actual_cost
        ,source.origin
        ,source.destination
        ,source.seen
        ,source.seen
        FROM etl.stage_trip AS source 
        WHERE source.batch = '{{ ts_nodash }}'
        AND NOT EXISTS
        (
            SELECT 1
            FROM fact.trip AS target
            WHERE target.trip_id = source.trip_id
        )
        """
        super().__init__(sql, *args, **kwargs)
