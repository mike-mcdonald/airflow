import hashlib
import json
import pathlib
import os

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,  timedelta
from math import atan2, pi, pow, sqrt
from numpy import nan

import geopandas as gpd
import numpy as np
import pandas as pd

from pytz import timezone
from shapely.wkt import loads

from airflow.models import BaseOperator

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook
from common_plugins.dataframe_plugin.hooks.azure_mssql_dataframe_hook import AzureMsSqlDataFrameHook


class MobilityTripsToSqlExtractOperator(BaseOperator):
    """

    """

    template_fields = ('trips_local_path',
                       'trips_remote_path',
                       'segment_hits_local_path',
                       'segment_hits_remote_path',
                       'cities_local_path',
                       'cities_remote_path',
                       'parking_districts_local_path',
                       'parking_districts_remote_path',
                       'pattern_areas_local_path',
                       'pattern_areas_remote_path',)

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 sql_conn_id="azure_sql_server_default",
                 data_lake_conn_id="azure_data_lake_default",
                 trips_local_path=None,
                 trips_remote_path=None,
                 segment_hits_local_path=None,
                 segment_hits_remote_path=None,
                 cities_local_path=None,
                 cities_remote_path=None,
                 parking_districts_local_path=None,
                 parking_districts_remote_path=None,
                 pattern_areas_local_path=None,
                 pattern_areas_remote_path=None,
                 df_global=None,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.data_lake_conn_id = data_lake_conn_id
        self.trips_local_path = trips_local_path
        self.trips_remote_path = trips_remote_path
        self.segment_hits_local_path = segment_hits_local_path
        self.segment_hits_remote_path = segment_hits_remote_path
        self.cities_local_path = cities_local_path
        self.cities_remote_path = cities_remote_path
        self.parking_districts_local_path = parking_districts_local_path
        self.parking_districts_remote_path = parking_districts_remote_path
        self.pattern_areas_local_path = pattern_areas_local_path
        self.pattern_areas_remote_path = pattern_areas_remote_path

    def execute(self, context):
        end_time = context.get("execution_date")
        pace = timedelta(hours=2) if datetime.now().date() > context.get(
            "execution_date").date() else timedelta(hours=12)
        start_time = end_time - pace

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
        trips['seen'] = trips.seen.dt.round("L")
        trips['seen'] = trips.seen.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        trips['propulsion_type'] = trips.propulsion_type.map(
            lambda x: ','.join(sorted(x)))
        trips['start_time'] = trips.start_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        trips['start_time'] = trips.start_time.dt.round("L")
        trips['start_time'] = trips.start_time.map(
            lambda x: datetime.replace(x, tzinfo=None))  # Remove timezone info after shifting
        trips['start_date_key'] = trips.start_time.map(
            lambda x: int(x.strftime('%Y%m%d')))
        trips['start_time'] = trips.start_time.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        trips['end_time'] = trips.end_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        trips['end_time'] = trips.end_time.dt.round("L")
        trips['end_time'] = trips.end_time.map(
            lambda x: datetime.replace(x, tzinfo=None))
        trips['end_date_key'] = trips.end_time.map(
            lambda x: int(x.strftime('%Y%m%d')))
        trips['end_time'] = trips.end_time.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

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
        trips['route'] = trips.route.map(lambda x: x.dropna(axis=0, subset=['geometry']))  #remove all rows for which the value of geometry is NaN
        self.log.debug("Retrieving origin and destination...")

        def get_origin(route):
            return route.loc[route['timestamp'].idxmin()].geometry

        def get_destination(route):
            return route.loc[route['timestamp'].idxmax()].geometry or route.loc[route["timestamp"].idxmin()].geometry
        # Pull out the origin and destination
        trips['origin'] = trips.route.map(get_origin)
        trips['destination'] = trips.route.map(get_destination)

        self.log.debug("Extracting route dataframe...")

        route_df = gpd.GeoDataFrame(
            pd.concat(trips.route.values, sort=False).sort_values(
                by=['trip_id', 'timestamp'], ascending=True
            )
        ).reset_index(drop=True)
        route_df.crs = {'init': 'epsg:4326'}
        route_df['datetime'] = route_df.timestamp.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))
        route_df['datetime'] = route_df.datetime.dt.round("L")
        route_df['datetime'] = route_df.datetime.map(
            lambda x: datetime.replace(x, tzinfo=None))
        route_df['date_key'] = route_df.datetime.map(
            lambda x: int(x.strftime('%Y%m%d')))
        # Generate a hash to aid in merge operations
        route_df['hash'] = route_df.apply(lambda x: hashlib.md5((
            x.trip_id + x.provider_id + x.datetime.strftime('%d%m%Y%H%M%S%f')
        ).encode('utf-8')).hexdigest(), axis=1)
        route_df['datetime'] = route_df.datetime.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

        # delete before passing to dataframe write, segmentation fault otherwise
        del trips["route"]

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

        trips = trips.set_geometry('origin')
        trips['start_cell_key'] = gpd.sjoin(
            trips.set_geometry('origin'), cells, how="left", op="within")['key'] # why set_geometry again?

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.data_lake_conn_id
        )

        def set_df_globally(local_path, remote_path):
            df = hook.download_file(
                local_path, remote_path) 
            df = gpd.read_file(local_path)
            df['geometry'] = df.wkt.map(loads)
            df.crs = {'init': 'epsg:4326'}
            self.df_global

        def find_geospatial_dim(local_path, remote_path):
            pathlib.Path(os.path.dirname(local_path)
                         ).mkdir(parents=True, exist_ok=True)
            if (self.df_global == None):
                set_df_globally(local_path,remote_path)
            series = gpd.sjoin(
                trips.copy(), self.df_global, how="left", op="within")['key']

            os.remove(local_path)

            return series

        with ThreadPoolExecutor(max_workers=3) as executor:
            start_city_key = executor.submit(find_geospatial_dim,
                                             self.cities_local_path, self.cities_remote_path)
            start_parking_district_key = executor.submit(find_geospatial_dim,
                                                         self.parking_districts_local_path, self.parking_districts_remote_path)
            start_pattern_area_key = executor.submit(find_geospatial_dim,
                                                     self.pattern_areas_local_path, self.pattern_areas_remote_path)

            trips['start_city_key'] = start_city_key.result()
            trips['start_parking_district_key'] = start_parking_district_key.result()
            trips['start_pattern_area_key'] = start_pattern_area_key.result()

            self.df_global=None
            del start_city_key
            del start_parking_district_key
            del start_pattern_area_key
        trips = trips.set_geometry('destination')
        trips['end_cell_key'] = gpd.sjoin(
            trips, cells, how="left", op="within")['key']
        del cells

        with ThreadPoolExecutor(max_workers=3) as executor:
            end_city_key = executor.submit(find_geospatial_dim,
                                           self.cities_local_path, self.cities_remote_path)
            end_parking_district_key = executor.submit(find_geospatial_dim,
                                                       self.parking_districts_local_path, self.parking_districts_remote_path)
            end_pattern_area_key = executor.submit(find_geospatial_dim,
                                                   self.pattern_areas_local_path, self.pattern_areas_remote_path)

            trips['end_city_key'] = end_city_key.result()
            trips['end_parking_district_key'] = end_parking_district_key.result()
            trips['end_pattern_area_key'] = end_pattern_area_key.result()

            self.df_global=None
            del end_city_key
            del end_parking_district_key
            del end_pattern_area_key

        self.log.debug("Writing trips extract to data lake...")

        pathlib.Path(os.path.dirname(self.trips_local_path)
                     ).mkdir(parents=True, exist_ok=True)

        trips['standard_cost'] = trips['standard_cost'] if 'standard_cost' in trips else np.nan
        trips['actual_cost'] = trips['actual_cost'] if 'actual_cost' in trips else np.nan
        trips['parking_verification_url'] = trips['parking_verification_url'] if 'parking_verification_url' in trips else np.nan

        trips[[
            'trip_id',
            'provider_id',
            'provider_name',
            'device_id',
            'vehicle_id',
            'vehicle_type',
            'propulsion_type',
            'start_time',
            'start_date_key',
            'start_cell_key',
            'start_city_key',
            'start_parking_district_key',
            'start_pattern_area_key',
            'end_time',
            'end_date_key',
            'end_cell_key',
            'end_city_key',
            'end_parking_district_key',
            'end_pattern_area_key',
            'distance',
            'duration',
            'accuracy',
            'standard_cost',
            'actual_cost',
            'parking_verification_url',
            'seen',
            'batch'
        ]].to_csv(self.trips_local_path, index=False)

        hook.upload_file(self.trips_local_path, self.trips_remote_path)
        hook.set_expiry(self.trips_remote_path, 'RelativeToNow',
                        expire_time=(72 * 3600 * 1000))

        os.remove(self.trips_local_path)

        self.log.debug("Reading segments from data warehouse...")

        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id
        )

        # Break out segment hits
        segments = hook.read_table_dataframe(
            table_name="segment", schema="dim")
        segments['geometry'] = segments.wkt.map(lambda g: loads(g))
        segments = gpd.GeoDataFrame(segments)
        segments.crs = {'init': 'epsg:4326'}

        self.log.debug("Mapping routes to segments...")

        route_df['segment_key'] = gpd.sjoin(
            route_df, segments, how="left", op="within"
        )[['hash', 'key']].drop_duplicates(subset='hash')['key']

        self.log.debug("Measuring route characteristics...")
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

        self.log.debug("Writing routes to data lake...")

        route_df = route_df.drop_duplicates(
            subset=['segment_key', 'trip_id'], keep='last')

        del route_df["trip_id"]

        route_df[[
            'provider_id',
            'date_key',
            'segment_key',
            'hash',
            'datetime',
            'vehicle_type',
            'propulsion_type',
            'heading',
            'speed',
            'seen',
            'batch'
        ]].to_csv(self.segment_hits_local_path, index=False)

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.data_lake_conn_id
        )

        hook.upload_file(self.segment_hits_local_path,
                         self.segment_hits_remote_path)
        hook.set_expiry(self.segment_hits_remote_path, 'RelativeToNow',
                        expire_time=(72 * 3600 * 1000))

        os.remove(self.segment_hits_local_path)

        return
