from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityVehicleSyncOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 source_table=None,
                 * args, **kwargs):
        sql = f"""
        insert into
            dim.vehicle (
                device_id,
                vehicle_id,
                vehicle_type
            )
        select distinct
            device_id,
            vehicle_id,
            vehicle_type
        from
            {source_table} as e
        where
            batch = '{{{{ ts_nodash }}}}'
        and not exists (
            select
                1
            from
                dim.vehicle as v
            where
                v.device_id = e.device_id
            and v.vehicle_id = e.vehicle_id
        )
        """
        super().__init__(sql, *args, **kwargs)
