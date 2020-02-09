from common_plugins.dataframe_plugin.hooks.sql_dataframe_hook import SqlDataFrameHook


class PgSqlDataFrameHook(SqlDataFrameHook):
    def __init__(self,
                 pgsql_conn_id='sql_server_default',
                 *args, **kwargs):
        super().__init__(sql_conn_id=pgsql_conn_id, *args, **kwargs)

    def get_connection_string(self, connection):
        return f'postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}'
