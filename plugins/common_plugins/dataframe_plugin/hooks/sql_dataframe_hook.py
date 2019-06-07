import pandas as pd
import sqlalchemy

from airflow.hooks.base_hook import BaseHook


class SqlDataFrameHook(BaseHook):
    def __init__(self,
                 sql_conn_id="database_default",
                 *args, **kwargs):
        self.conn_id = sql_conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        connection = self.get_connection(self.conn_id)
        connection_string = self.get_connection_string(connection)
        engine = sqlalchemy.engine.create_engine(
            connection_string,
            # Following avoids error about transaction on connect with Azure SQL Warehouse
            isolation_level='AUTOCOMMIT')
        engine.connect()
        return engine

    def get_connection_string(self, connection):
        raise NotImplementedError

    def write_dataframe(self,
                        dataframe,
                        table_name,
                        schema=None,
                        if_exists='append',
                        index=False,
                        index_label=None,
                        chunksize=None,
                        dtype=None,
                        method=None):
        dataframe.to_sql(name=table_name,
                         con=self.connection,
                         schema=schema,
                         if_exists=if_exists,
                         index=index,
                         index_label=index_label,
                         chunksize=chunksize,
                         dtype=dtype,
                         method=method)

    def read_dataframe(self,
                       table_name,
                       schema=None):
        return pd.read_sql_table(table_name=table_name,
                                 con=self.connection,
                                 schema=schema)
