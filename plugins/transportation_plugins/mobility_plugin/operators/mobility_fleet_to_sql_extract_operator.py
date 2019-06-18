from datetime import datetime, timedelta

import pandas as pd

from airflow.models import BaseOperator

from common_plugins.dataframe_plugin.hooks.azure_mssql_dataframe_hook import AzureMsSqlDataFrameHook


class MobilityFleetToSqlExtractOperator(BaseOperator):
    """

    """

    def __init__(self,
                 sql_conn_id="azure_sql_server_default",
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id

    def execute(self, context):
        end_time = context.get("execution_date")
        start_time = end_time - timedelta(hours=48)

        date_index = pd.date_range(
            start=start_time, end=end_time.naive(), freq='T')

        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id)

        providers = hook.read_table_dataframe(
            table_name="provider", schema="dim")

        index = pd.MultiIndex.from_product(
            [date_index.values, providers.key.values], names=["time", "provider_key"])

        fleet_df = pd.DataFrame(index=index).reset_index()
        fleet_df['batch'] = context.get("ts_nodash")
        fleet_df['date_key'] = fleet_df.time.map(
            lambda x: int(x.strftime('%Y%m%d')))

        hook.write_dataframe(
            fleet_df, table_name="extract_fleet_count", schema="etl")

        return
