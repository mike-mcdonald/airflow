import json
from datetime import datetime,  timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MSSqlDataFrameHook


class MobilityTripsToSqlTablesOperator(BaseOperator):
    """

    """

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 sql_conn_id="sql_server_default",
                 remote_path="",
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id,
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.remote_path = remote_path

    def execute(self, context):
        end_time = context.execution_date
        start_time = end_time - timedelta(hours=12)

        # Create the hook
        hook = MobilityProviderHook(
            mobility_provider_conn_id=self.mobility_provider_conn_id,
            mobility_provider_token_conn_id=self.mobility_provider_token_conn_id)

        # Get trips as a DataFrame
        trips = hook.get_trips(
            min_end_time=start_time, max_end_time=end_time)

        trips['seen'] = datetime.now()
        trips['propulsion_type'] = trips.propulsion_type.map(
            lambda x: ','.join(x))
        trips['parking_verification_url'] = trips.parking_verification_url.map(
            lambda x: ','.join(x))

        # Convert the route to a DataFrame now to make mapping easier
        trips['route'] = trips.route.map(
            lambda r: gpd.GeoDataFrame.from_features(r['features']))

        def get_origin(route):
            return route.loc[route['timestamp'].idxmin()].geometry

        def get_destination(route):
            return route.loc[route['timestamp'].idxmax()].geometry

        # Pull out the origin and destination
        trips['origin'] = trips.route.map(get_origin)
        trips['destination'] = trips.route.map(get_destination)

        hook = MSSqlDataFrameHook(
            sql_conn_id=self.sql_conn_id
        )

        # Break out segment hits
        cells = hook.read_dataframe(table_name="cells", schema="dim")
        cells['geometry'] = cells.wkt.map(lambda g: loads(g))
        cells = gpd.GeoDataFrame(cells)

        trips['origin'] = gpd.sjoin(
            trips.set_geometry('origin'), cells, how="left", op="intersects")['index_right']
        trips['destination'] = gpd.sjoin(
            trips.set_geometry('destination'), cells, how="left", op="intersects")['index_right']

        del cells

        hook.write_dataframe(
            trips,
            table_name='stage_trip',
            schema='etl'
        )

        segments = hook.read_dataframe(table_name="segments", schema="dim")
        segments['geometry'] = segments.wkt.map(lambda g: loads(g))
        segments = gpd.GeoDataFrame(segments)

        def parse_route(trip):
            frame = trip.route
            frame['provider_id'] = trip.provider_id
            frame['provider_name'] = trip.provider_name
            frame['vehicle_type'] = trip.vehicle_type
            frame['propulsion_type'] = ','.join(trip.propulsion_type)
            return frame

        route_df = trips.apply(parse_route, axis=1)

        del trips

        route_df['segment'] = gpd.sjoin(
            route_df, segments, how="left", op="intersects")['index_right']

        del route_df['geometry']
        del segments

        hook.write_dataframe(
            route_df,
            table_name='stage_segmenthit',
            schema='etl'
        )

        return
