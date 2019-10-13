from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityProviderSyncOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 source_table=None,
                 * args, **kwargs):
        sql = f"""
        insert into
            dim.provider (
                provider_id,
                provider_name
            )
        select distinct
            provider_id,
            provider_name
        from
            {source_table} as e
        where
            batch = '{{{{ ts_nodash }}}}'
        and not exists (
            select
                1
            from
                dim.provider as p
            where
                p.provider_id = e.provider_id
        )
        """
        super().__init__(sql, *args, **kwargs)
