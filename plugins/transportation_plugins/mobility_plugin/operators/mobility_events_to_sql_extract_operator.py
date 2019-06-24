import hashlib
import json
import pathlib
import os

from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from shapely.geometry import Point
from shapely.wkt import loads

from airflow.models import BaseOperator

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook
from common_plugins.dataframe_plugin.hooks.azure_mssql_dataframe_hook import AzureMsSqlDataFrameHook


class MobilityEventsToSqlExtractOperator(BaseOperator):
    """

    """

    template_fields = ('events_local_path', 'events_remote_path',)

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 sql_conn_id="azure_sql_server_default",
                 data_lake_conn_id="azure_data_lake_default",
                 events_local_path=None,
                 events_remote_path=None,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.data_lake_conn_id = data_lake_conn_id
        self.events_local_path = events_local_path
        self.events_remote_path = events_remote_path

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
        events['seen'] = events.seen.dt.round('L')

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
        events['event_time'] = events.event_time.map(lambda x: x.replace(
            tzinfo=None))  # Remove timezone info after shifting
        events['event_time'] = events.event_time.dt.round('L')
        events['date_key'] = events.event_time.map(
            lambda x: int(x.strftime('%Y%m%d')))
        events['event_hash'] = events.apply(
            lambda x: hashlib.md5(
                f"{x.device_id}{x.event_type_reason}{x.event_time.strftime('%d%m%Y%H%M%S%f')}".encode(
                    'utf-8')
            ).hexdigest(), axis=1)
        events['event_time'] = events.event_time.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

        events = events.drop_duplicates(subset='event_hash')

        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id)

        # Aggregate the location to a cell
        cells = hook.read_table_dataframe(table_name="cell", schema="dim")
        cells['geometry'] = cells.wkt.map(lambda g: loads(g))
        cells = gpd.GeoDataFrame(cells)
        cells.crs = {'init': 'epsg:4326'}

        events['event_location'] = gpd.sjoin(
            events, cells, how="left", op="within")['key']

        del cells

        events = events.rename(index=str, columns={
            'event_type': 'state',
            'event_type_reason': 'event'
        })

        pathlib.Path(os.path.dirname(self.events_local_path)
                     ).mkdir(parents=True, exist_ok=True)

        events['associated_trip'] = events['associated_trip'] if 'associated_trip' in events else np.nan

        events[[
            'event_hash',
            'provider_id',
            'provider_name',
            'device_id',
            'vehicle_id',
            'vehicle_type',
            'propulsion_type',
            'date_key',
            'event_time',
            'state',
            'event',
            'event_location',
            'battery_pct',
            'associated_trip',
            'seen',
            'batch'
        ]].to_csv(self.events_local_path, index=False)

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.data_lake_conn_id
        )

        hook.upload_file(self.events_local_path, self.events_remote_path)
        hook.set_expiry(self.events_remote_path, 'RelativeToNow',
                        expire_time=(72 * 3600 * 1000))

        os.remove(self.events_local_path)

        return
