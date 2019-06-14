import pyodbc

from airflow.hooks.dbapi_hook import DbApiHook


class MsSqlHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.
    """

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MsSqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        conn = self.get_connection(self.mssql_conn_id)
        conn = pyodbc.connect(
            "DRIVER={0};SERVER={1};PORT={2};DATABASE={3};UID={4};PWD={5};Encrypt=yes"
            .format(
                '{ODBC Driver 17 for SQL Server}'
                , conn.host
                , conn.port
                , self.schema or conn.schema
                , conn.login
                , conn.password
            )
        )

        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit = autocommit