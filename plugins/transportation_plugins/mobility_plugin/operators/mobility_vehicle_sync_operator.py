from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityVehicleSyncOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 source_table=None,
                 * args, **kwargs):
        sql = f"""
        INSERT INTO dim.vehicle (
            device_id
            ,vehicle_id
            ,vehicle_type
        )
        SELECT DISTINCT
        device_id
        ,vehicle_id
        ,vehicle_type
        FROM {source_table} AS e
        WHERE batch = '{{{{ ts_nodash }}}}'
        AND NOT EXISTS (
            SELECT 1
            FROM dim.vehicle AS v
            WHERE v.device_id = e.device_id
            AND v.vehicle_id = e.vehicle_id
        )
        """
        super().__init__(sql, *args, **kwargs)
