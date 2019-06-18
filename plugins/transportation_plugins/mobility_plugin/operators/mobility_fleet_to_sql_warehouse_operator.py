from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityFleetToSqlWarehouseOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 * args, **kwargs):
        sql = """
        UPDATE fact.fleet_count
        SET available = source.available,
        reserved = source.reserved,
        unavailable = source.unavailable,
        removed = source.removed
        FROM etl.stage_fleet_count AS source
        WHERE source.batch = '{{ ts_nodash }}
        AND source.provider_key = fact.fleet_count.provider_key
        AND source.time = fact.fleet_count.time

        INSERT fact.fleet_count (
            date_key
            ,provider_key
            ,time
            ,available
            ,reserved
            ,unavailable
            ,removed
        )  
        SELECT source.date_key
        ,source.provider_key
        ,source.time
        ,source.available
        ,source.reserved
        ,source.unavailable
        ,source.removed
        FROM etl.stage_fleet_count AS source
        WHERE source.batch = '{{ ts_nodash }}
        AND NOT EXISTS
        (
            SELECT 1
            FROM fact.fleet_count AS target
            WHERE source.provider_key = target.provider_key
            AND source.time = target.time
        )
        """
        super().__init__(sql, *args, **kwargs)
