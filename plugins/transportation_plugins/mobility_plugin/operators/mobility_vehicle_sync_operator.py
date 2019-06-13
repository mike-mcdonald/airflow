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
        )
        SELECT
        device_id
        ,vehicle_id
        FROM {source_table}
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim.vehicle AS v
            WHERE v.device_id = device_id
            AND v.vehicle_id = vehicle_id
        )
        """
        super().__init__(sql, *args, **kwargs)
