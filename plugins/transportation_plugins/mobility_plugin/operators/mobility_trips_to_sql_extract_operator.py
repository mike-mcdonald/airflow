import hashlib
import json
from datetime import datetime,  timedelta
from math import atan2, cos, pow, sin, sqrt
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MsSqlDataFrameHook


class MobilityTripsToSqlExtractOperator(BaseOperator):
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

        hook = MsSqlDataFrameHook(
            sql_conn_id=self.sql_conn_id
        )
        # Map to cells
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
            table_name='extract_trip',
            schema='etl'
        )

        # Break out segment hits
        segments = hook.read_dataframe(table_name="segments", schema="dim")
        segments['geometry'] = segments.wkt.map(lambda g: loads(g))
        segments = gpd.GeoDataFrame(segments)

        def parse_route(trip):
            frame = trip.route
            frame['trip_id'] = trip.trip_id
            frame['provider_id'] = trip.provider_id
            frame['provider_name'] = trip.provider_name
            frame['vehicle_type'] = trip.vehicle_type
            frame['propulsion_type'] = ','.join(trip.propulsion_type)
            return frame

        route_df = trips.apply(parse_route, axis=1).sort_values(
            by=['timestamp'], ascending=True
        )

        route_df['segment'] = gpd.sjoin(
            route_df, segments, how="left", op="intersects")['index_right']

        del segments

        # Generate a hash to aid in merge operations
        route_df['hash'] = trips.apply(lambda x: hashlib.md5((
            x.trip_id + x.provider_id + x.timestamp.strftime('%d%m%Y%H%M%S%f')
        ).encode('utf-8')))

        del trips

        def find_bearing(trip):
            lat1 = trip.geometry.y
            lng1 = trip.geometry.x
            lat2 = trip.next_geometry.y
            lng2 = trip.next_geometry.x

            dlng = lng2 - lng1

            x = cos(lat2) * sin(dlng)
            y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlng)

            return atan2(x, y)

        def find_speed(trip):
            y1 = trip.geometry.y
            x1 = trip.geometry.x
            y2 = trip.next_geometry.y
            x2 = trip.next_geometry.x

            d = sqrt(pow((x2 - x1), 2) + pow((y2 - y1), 2))
            # timestamp is in milliseconds
            dt = (trip.next_timestamp - trip.timestamp) / 1000

            return d / dt

        route_df['next_geometry'] = route_df.geometry.shift(1)

        reoute_df['heading'] = route_df.apply(find_bearing, axis=1)

        # Swtich to mercator to measure in meters
        route_df.geometry.crs = {'init': 'epsg:3857'}
        route_df.next_geometry.crs = {'init': 'epsg:3857'}

        route_df['next_timestamp'] = route_df.timestamp.shift(1)

        route_df['speed'] = route_df.apply(find_speed, axis=1)

        del route_df['geometry']
        del route_df['next_geometry']
        del route_df['next_timestamp']

        route_df = route_df.drop_duplicates(subset=['segment', 'trip_id'])

        del route_df['trip_id']

        hook.write_dataframe(
            route_df,
            table_name='extract_segmenthit',
            schema='etl'
        )

        return
