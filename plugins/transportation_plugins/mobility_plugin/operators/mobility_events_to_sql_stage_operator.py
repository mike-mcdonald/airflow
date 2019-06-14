from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityEventsToSqlStageOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 * args, **kwargs):
        sql = """\
        INSERT INTO etl.stage_state (
            provider_key
            ,vehicle_key
            ,propulsion_type
            ,start_state
            ,start_event
            ,start_time
            ,start_location
            ,start_battery_pct
            ,end_state
            ,end_event
            ,end_time
            ,end_location
            ,end_battery_pct
            ,associated_trip
        )
        SELECT
        p.[key]
        ,v.[key]
        ,propulsion_type
        ,state
        ,event
        ,event_time
        ,event_location
        ,battery_pct
        ,LEAD(state) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(event) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(event_time) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(event_location) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,LEAD(battery_pct) OVER(PARTITION BY e.device_id ORDER BY event_time)
        ,COALESCE(associated_trip, LEAD(associated_trip) OVER(PARTITION BY e.device_id ORDER BY event_time))
        FROM etl.extract_event AS e
        LEFT JOIN dim.provider AS p ON p.provider_id = e.provider_id
        LEFT JOIN dim.vehicle AS v ON v.device_id = e.device_id
        WHERE e.batch = '{{ ts_nodash }}'
        """
        super().__init__(sql, *args, **kwargs)
