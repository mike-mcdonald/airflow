import hashlib
import json

from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd

from pytz import timezone
from shapely.geometry import Point
from shapely.wkt import loads

from airflow.models import BaseOperator

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.azure_mssql_dataframe_hook import AzureMsSqlDataFrameHook


class MobilityEventsToSqlExtractOperator(BaseOperator):
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
        self.mobility_provider_conn_id = mobility_provider_conn_id
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id

    def execute(self, context):
        end_time = context.get("execution_date")
        start_time = end_time - timedelta(hours=12)

        # Create the hook
        hook = MobilityProviderHook(
            mobility_provider_conn_id=self.mobility_provider_conn_id,
            mobility_provider_token_conn_id=self.mobility_provider_token_conn_id)

        # Get trips as a DataFrame
        events = gpd.GeoDataFrame(hook.get_events(
            start_time=start_time, end_time=end_time))

        if len(events) <= 0:
            self.log.warning(
                f"Received no events for time period {start_time} to {end_time}")
            return

        events['batch'] = context.get("ts_nodash")
        events['seen'] = datetime.now()
        # Get the GeoDataFrame configured correctly
        events['event_location'] = events.event_location.map(
            lambda x: x['geometry']['coordinates'])
        events['event_location'] = events.event_location.apply(Point)
        events = events.set_geometry('event_location')
        events.crs = {'init': 'epsg:4326'}

        events['propulsion_type'] = events.propulsion_type.map(
            lambda x: ','.join(sorted(x)))
        events['event_time'] = events.event_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        events['event_hash'] = events.apply(
            lambda x: hashlib.md5(
                f"{x.provider_id}{x.device_id}{x.event_time.strftime('%d%m%Y%H%M%S%f')}".encode(
                    'utf-8')
            ).hexdigest(), axis=1)

        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id)

        # Aggregate the location to a cell
        cells = hook.read_dataframe(table_name="cell", schema="dim")
        cells['geometry'] = cells.wkt.map(lambda g: loads(g))
        cells = gpd.GeoDataFrame(cells)
        cells.crs = {'init': 'epsg:4326'}

        events['event_location'] = gpd.sjoin(
            events, cells, how="left", op="intersects")['key']

        del cells

        events = events.rename(index=str, columns={
            'event_type': 'state',
            'event_type_reason': 'event'
        })

        hook.write_dataframe(
            events, table_name="extract_event", schema="etl")

        return
