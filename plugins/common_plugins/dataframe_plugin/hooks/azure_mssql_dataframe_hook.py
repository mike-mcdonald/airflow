from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MsSqlDataFrameHook


class AzureMsSqlDataFrameHook(MsSqlDataFrameHook):
    def __init__(self,
                 azure_mssql_conn_id="azure_sql_server_default",
                 *args, **kwargs):
        super().__init__(sql_conn_id=azure_mssql_conn_id, *args, **kwargs)

    def get_connection_string(self, connection):
        return f"mssql+pyodbc://{connection.login}@{connection.host}:{connection.password}@{connection.host}.database.windows.net:{connection.port}/{connection.database}?driver=ODBC+Driver+17+for+SQL+Server"
