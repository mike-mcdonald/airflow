import hashlib
import json
from datetime import datetime,  timedelta
from math import atan2, pi, pow, sqrt
from numpy import nan

import geopandas as gpd
import pandas as pd

from pytz import timezone
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
        trips.crs = {'init': 'epsg:4326'}

        if len(trips) <= 0:
            self.log.warning(
                f"Received no trips for time period {start_time} to {end_time}")
            return

        trips = trips.rename(index=str, columns={
            'trip_duration': 'duration',
            'trip_distance': 'distance'
        })

        trips['batch'] = context.get("ts_nodash")
        trips['seen'] = datetime.now()
        trips['propulsion_type'] = trips.propulsion_type.map(
            lambda x: ','.join(sorted(x)))
        trips['start_time'] = trips.start_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        trips['start_date_key'] = trips.start_time.map(
            lambda x: int(x.strftime('%Y%m%d')))
        trips['end_time'] = trips.end_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        trips['end_date_key'] = trips.end_time.map(
            lambda x: int(x.strftime('%Y%m%d')))

        self.log.debug("Converting route to a GeoDataFrame...")
        # Convert the route to a DataFrame now to make mapping easier
        trips['route'] = trips.route.map(
            lambda r: gpd.GeoDataFrame.from_features(r['features']))

        def parse_route(trip):
            frame = trip.route
            frame['batch'] = trip.batch
            frame['trip_id'] = trip.trip_id
            frame['provider_id'] = trip.provider_id
            frame['vehicle_type'] = trip.vehicle_type
            frame['propulsion_type'] = trip.propulsion_type
            frame['seen'] = trip.seen
            return frame

        trips["route"] = trips.apply(parse_route, axis=1)

        self.log.debug("Retrieving origin and destination...")

        def get_origin(route):
            return route.loc[route['timestamp'].idxmin()].geometry

        def get_destination(route):
            return route.loc[route['timestamp'].idxmax()].geometry

        # Pull out the origin and destination
        trips['origin'] = trips.route.map(get_origin)
        trips['destination'] = trips.route.map(get_destination)

        self.log.debug("Extracting route dataframe...")

        route_df = gpd.GeoDataFrame(pd.concat(trips.route.values, sort=False).sort_values(
            by=['trip_id', 'timestamp'], ascending=True
        ))
        route_df.crs = {'init': 'epsg:4326'}
        route_df['datetime'] = route_df.timestamp.map(lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        route_df['date_key'] = route_df.datetime.map(
            lambda x: int(x.strftime('%Y%m%d')))
        # Generate a hash to aid in merge operations
        route_df['hash'] = route_df.apply(lambda x: hashlib.md5((
            x.trip_id + x.provider_id + x.datetime.strftime('%d%m%Y%H%M%S%f')
        ).encode('utf-8')).hexdigest(), axis=1)

        del trips["route"] # delete before passing to dataframe write, segmentation fault otherwise

        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id
        )

        self.log.debug("Reading cells from data warehouse...")

        # Map to cells
        cells = hook.read_table_dataframe(table_name="cell", schema="dim")
        cells['geometry'] = cells.wkt.map(loads)
        cells = gpd.GeoDataFrame(cells)
        cells.crs = {'init': 'epsg:4326'}

        self.log.debug("Mapping trip O/D to cells...")

        trips['origin'] = gpd.sjoin(
            trips.set_geometry('origin'), cells, how="left", op="within")['key']
        trips['destination'] = gpd.sjoin(
            trips.set_geometry('destination'), cells, how="left", op="within")['key']

        del cells

        self.log.debug("Writing trips extract to data warehouse...")

        hook.write_dataframe(
            trips,
            table_name='extract_trip',
            schema='etl'
        )

        self.log.debug("Reading segments from data warehouse...")

        # Break out segment hits
        segments = hook.read_table_dataframe(
            table_name="segment", schema="dim")
        segments['geometry'] = segments.wkt.map(lambda g: loads(g))
        segments = gpd.GeoDataFrame(segments)
        segments.crs = {'init': 'epsg:4326'}

        self.log.debug("Mapping routes to segments...")

        route_df['segment_key'] = gpd.sjoin(
            route_df, segments, how="left", op="intersects")['key']

        # Swtich to mercator to measure in meters
        route_df = route_df.to_crs(epsg=3857)

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
        route_df = route_df.dropna()

        route_df['dx'] = route_df.apply(
            lambda x: x.nx - x.x, axis=1)
        route_df['dy'] = route_df.apply(
            lambda x: x.ny - x.y, axis=1)
        route_df['dt'] = route_df.apply(
            lambda x: (x.next_timestamp - x.timestamp) / 1000, axis=1)

        del route_df['x']
        del route_df['y']
        del route_df['nx']
        del route_df['ny']
        del route_df["timestamp"]
        del route_df["next_timestamp"]

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
            subset=['segment_key', 'trip_id'], keep='last')

        del route_df["trip_id"]

        hook.write_dataframe(
            route_df,
            table_name='extract_segment_hit',
            schema='etl'
        )

        return
