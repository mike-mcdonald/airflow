import hashlib
import json
from datetime import datetime,  timedelta
from math import atan2, pi, pow, sqrt
from numpy import nan
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

from airflow.models import BaseOperator

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.azure_mssql_dataframe_hook import AzureMsSqlDataFrameHook


class MobilityTripsToSqlExtractOperator(BaseOperator):
    """

    """

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 sql_conn_id="azure_sql_server_default",
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

        # Get trips as a GeoDataFrame
        trips = gpd.GeoDataFrame(hook.get_trips(
            min_end_time=start_time, max_end_time=end_time))

        if len(trips) <= 0:
            self.log.warning(
                f"Received no trips for time period {start_time} to {end_time}")
            return

        trips['seen'] = datetime.now()
        trips['propulsion_type'] = trips.propulsion_type.map(
            lambda x: ','.join(x))
        trips['start_time'] = trips.start_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        trips['end_time'] = trips.end_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))

        # Convert the route to a DataFrame now to make mapping easier
        trips['route'] = trips.route.map(
            lambda r: gpd.GeoDataFrame.from_features(r['features']))

        def get_origin(route):
            return route.loc[route['timestamp'].idxmin()].geometry

        def get_destination(route):
            return route.loc[route['timestamp'].idxmax()].geometry

        # Pull out the origin and destination
        trips['origin'] = trips.route.map(get_origin)
        trips.origin.crs = {'init': 'epsg:4326'}
        trips['destination'] = trips.route.map(get_destination)
        trips.destination.crs = {'init': 'epsg:4326'}

        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id
        )
        # Map to cells
        cells = hook.read_dataframe(table_name="cell", schema="dim")
        cells['geometry'] = cells.wkt.map(lambda g: loads(g))
        cells = gpd.GeoDataFrame(cells)
        cells.crs = {'init': 'epsg:4326'}

        trips['origin'] = gpd.sjoin(
            trips.set_geometry('origin'), cells, how="left", op="intersects")['key']
        trips['destination'] = gpd.sjoin(
            trips.set_geometry('destination'), cells, how="left", op="intersects")['key']

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
        segments.crs = {'init': 'epsg:4326'}

        def parse_route(trip):
            frame = trip.route
            frame['trip_id'] = trip.trip_id
            frame['provider_id'] = trip.provider_id
            frame['provider_name'] = trip.provider_name
            frame['vehicle_type'] = trip.vehicle_type
            frame['propulsion_type'] = ','.join(trip.propulsion_type)
            return frame

        route_df = trips.apply(parse_route, axis=1).sort_values(
            by=['trip_id', 'timestamp'], ascending=True
        )

        # Swtich to mercator to measure in meters
        route_df.crs = {'init': 'epsg:4326'}
        route_df = route_df.to_crs(epsg=3857)

        route_df['segment'] = gpd.sjoin(
            route_df, segments, how="left", op="intersects")['key']

        del segments
        del trips

        route_df['next_timestamp'] = route_df.groupby(
            'trip_id').timestamp.shift(-1)

        route_df['x'] = route_df.geometry.map(lambda g: g.x)
        route_df['y'] = route_df.geometry.map(lambda g: g.y)
        route_df['nx'] = route_df.groupby(['trip_id']).x.shift(-1)
        route_df['ny'] = route_df.groupby(['trip_id']).y.shift(-1)

        del route_df['geometry']

        # drop destination
        route_df.dropna()

        route_df['dx'] = route_df.apply(
            lambda x: x.nx - x.x, axis=1)
        route_df['dy'] = route_df.apply(
            lambda x: x.ny - x.y, axis=1)
        route_df['dt'] = route_df.apply(
            lambda x: (x.next_timestamp - x.timestamp).seconds, axis=1)  # timestamp is in milliseconds

        del route_df['x']
        del route_df['y']
        del route_df['nx']
        del route_df['ny']

        def find_heading(hit):
            deg = atan2(hit.dx, hit.dy) / pi * 180
            if deg < 0:
                deg = deg + 360
            return deg

        def find_speed(hit):
            if hit['dt'] <= 0:
                return 0

            d = sqrt(pow((hit.dx), 2) + pow((hit.dy), 2))

            return d / hit['dt']

        route_df['heading'] = route_df.apply(find_heading, axis=1)
        route_df['speed'] = route_df.apply(find_speed, axis=1)

        del route_df['dx']
        del route_df['dy']
        del route_df['dt']

        route_df = route_df.drop_duplicates(
            subset=['segment', 'trip_id'], keep='last')

        # Generate a hash to aid in merge operations
        route_df['hash'] = trips.apply(lambda x: hashlib.md5((
            x.trip_id + x.provider_id + x.timestamp.strftime('%d%m%Y%H%M%S%f')
        ).encode('utf-8').hexdigest()))

        del route_df['trip_id']

        hook.write_dataframe(
            route_df,
            table_name='extract_segmenthit',
            schema='etl'
        )

        return
