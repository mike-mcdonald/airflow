import json
from datetime import timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd

from shapely.wkt import loads

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.azure_sql_dataframe_hook import AzureSqlDataFrameHook


class MobilityEventsToSqlTableOperator(BaseOperator):
    """

    """

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 sql_conn_id="azure_sql_server_default",
                 table_name="",
                 schema="",
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id,
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id

    def execute(self, context):
        end_time = context.execution_date
        start_time = end_time - timedelta(hours=12)

        # Create the hook
        hook = MobilityProviderHook(
            mobility_provider_conn_id=self.mobility_provider_conn_id,
            mobility_provider_token_conn_id=self.mobility_provider_token_conn_id)

        # Get trips as a DataFrame
        events = hook.get_events(
            start_time=start_time, end_time=end_time)

        hook = AzureSqlDataFrameHook(
            sql_conn_id=self.sql_conn_id)

        # Aggregate the location to a cell
        cells = hook.read_dataframe(table_name="cells", schema="dim")
        cells['geometry'] = cells.wkt.map(lambda g: loads(g))
        cells = gpd.GeoDataFrame(cells)

        events['event_location'] = gpd.sjoin(
            events.set_geometry('event_location'), cells, how="left", op="intersects")['index_right']

        hook.write_dataframe(events, table_name="stage_events", schema="etl")

        return
