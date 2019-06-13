from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityProviderSyncOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                source_table=None,
                 * args, **kwargs):
        sql = f"""
        INSERT INTO dim.provider (
            provider_id
            ,provider_name
        )
        SELECT
        provider_id
        ,provider_name
        FROM {source_table}
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim.provider AS p
            WHERE p.provider_id = provider_id
        )
        """
        super().__init__(sql, *args, **kwargs)
