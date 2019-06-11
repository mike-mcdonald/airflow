from common_plugins.dataframe_plugin.hooks.sql_dataframe_hook import SqlDataFrameHook


class MsSqlDataFrameHook(SqlDataFrameHook):
    def __init__(self,
                 mssql_conn_id="sql_server_default",
                 *args, **kwargs):
        super().__init__(sql_conn_id=mssql_conn_id, *args, **kwargs)

    def get_connection_string(self, connection):
        return f"mssql+pyodbc://{connection.login}@{connection.host}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}?driver=ODBC+Driver+17+for+SQL+Server"
