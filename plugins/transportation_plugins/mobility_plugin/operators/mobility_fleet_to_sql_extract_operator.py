import pathlib
import os
from datetime import datetime, timedelta

import pandas as pd

from airflow.models import BaseOperator
from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook

from common_plugins.dataframe_plugin.hooks.azure_mssql_dataframe_hook import AzureMsSqlDataFrameHook


class MobilityFleetToSqlExtractOperator(BaseOperator):
    """

    """

    template_fields = ('fleet_local_path', 'fleet_remote_path',)

    def __init__(self,
                 sql_conn_id="azure_sql_server_default",
                 data_lake_conn_id="azure_data_lake_default",
                 fleet_local_path=None,
                 fleet_remote_path=None,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.data_lake_conn_id = data_lake_conn_id
        self.fleet_local_path = fleet_local_path
        self.fleet_remote_path = fleet_remote_path

    def execute(self, context):
        end_time = datetime.fromtimestamp(
            context.get("execution_date").timestamp())
        start_time = end_time - timedelta(hours=48)

        date_index = pd.date_range(
            start=start_time, end=end_time, freq='T')

        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id)

        providers = hook.read_sql_dataframe(
            sql="""
            SELECT [key]
            FROM dim.provider
            """
        )

        pathlib.Path(os.path.dirname(self.fleet_local_path)
                     ).mkdir(parents=True, exist_ok=True)

        index = pd.MultiIndex.from_product(
            [date_index.values, providers.key.values], names=["time", "provider_key"])

        fleet_df = pd.DataFrame(index=index).reset_index()
        fleet_df['seen'] = datetime.now()
        fleet_df['seen'] = fleet_df.seen.dt.round('L')
        fleet_df['seen'] = fleet_df.seen.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        fleet_df['date_key'] = fleet_df.time.map(
            lambda x: int(x.strftime('%Y%m%d')))
        fleet_df['time'] = fleet_df.time.dt.round('L')
        fleet_df['time'] = fleet_df.time.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        fleet_df['batch'] = context.get("ts_nodash")

        fleet_df[[
            'date_key',
            'provider_key',
            'time',
            'seen',
            'batch'
        ]].to_csv(self.fleet_local_path, index=False)

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.data_lake_conn_id
        )

        hook.upload_file(self.fleet_local_path, self.fleet_remote_path)

        os.remove(self.fleet_local_path)

        return
