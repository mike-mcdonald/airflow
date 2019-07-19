import hashlib
import json
import pathlib
import os

from concurrent.futures import ThreadPoolExecutor
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

    template_fields = ('events_local_path',
                       'events_remote_path',
                       'census_blocks_local_path',
                       'census_blocks_remote_path',
                       'cities_local_path',
                       'cities_remote_path',
                       'counties_local_path',
                       'counties_remote_path',
                       'neighborhoods_local_path',
                       'neighborhoods_remote_path',
                       'parks_local_path',
                       'parks_remote_path',
                       'parking_districts_local_path',
                       'parking_districts_remote_path',
                       'pattern_areas_local_path',
                       'pattern_areas_remote_path',
                       'zipcodes_local_path',
                       'zipcodes_remote_path')

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 sql_conn_id="azure_sql_server_default",
                 data_lake_conn_id="azure_data_lake_default",
                 events_local_path=None,
                 events_remote_path=None,
                 cities_local_path=None,
                 cities_remote_path=None,
                 parking_districts_local_path=None,
                 parking_districts_remote_path=None,
                 pattern_areas_local_path=None,
                 pattern_areas_remote_path=None,
                 
                 #New Geometry
                 census_blocks_local_path=None,
                 census_blocks_remote_path=None,
                 counties_local_path=None,
                 counties_remote_path=None,
                 neighborhoods_local_path=None,
                 neighborhoods_remote_path=None,
                 parks_local_path=None,
                 parks_remote_path=None,
                 zipcodes_local_path=None,
                 zipcodes_remote_path=None,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.data_lake_conn_id = data_lake_conn_id
        self.events_local_path = events_local_path
        self.events_remote_path = events_remote_path
        self.cities_local_path = cities_local_path
        self.cities_remote_path = cities_remote_path
        self.parking_districts_local_path = parking_districts_local_path
        self.parking_districts_remote_path = parking_districts_remote_path
        self.pattern_areas_local_path = pattern_areas_local_path
        self.pattern_areas_remote_path = pattern_areas_remote_path
        #New Geometry
        self.census_blocks_local_path = census_blocks_local_path
        self.census_blocks_remote_path = census_blocks_remote_path
        self.counties_local_path = counties_local_path
        self.counties_remote_path = counties_remote_path
        self.neighborhoods_local_path = neighborhoods_local_path
        self.neighborhoods_remote_path = neighborhoods_remote_path
        self.parks_local_path = parks_local_path
        self.parks_remote_path = parks_remote_path
        self.zipcodes_local_path = zipcodes_local_path
        self.zipcodes_remote_path = zipcodes_remote_path

    def execute(self, context):
        end_time = context.get("execution_date")
        pace = timedelta(hours=2) if datetime.now().date() > context.get(
            "execution_date").date() else timedelta(hours=12)
        start_time = end_time - pace

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

        events['event_time'] = events.event_time.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone("US/Pacific")))

        events['event_hash'] = events.apply(
            lambda x: hashlib.md5(
                f"{x.device_id}{x.event_type_reason}{x.event_time.strftime('%d%m%Y%H%M%S%f')}".encode(
                    'utf-8')
            ).hexdigest(), axis=1)

        events = events.drop_duplicates(subset='event_hash')

        events['batch'] = context.get("ts_nodash")
        events['seen'] = datetime.now()
        events['seen'] = events.seen.dt.round('L')
        events['seen'] = events.seen.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

        # Get the GeoDataFrame configured correctly
        events['event_location'] = events.event_location.map(
            lambda x: x['geometry']['coordinates'])
        events['event_location'] = events.event_location.apply(Point)
        events = events.set_geometry('event_location')
        events.crs = {'init': 'epsg:4326'}

        events['propulsion_type'] = events.propulsion_type.map(
            lambda x: ','.join(sorted(x)))
        events['event_time'] = events.event_time.map(lambda x: x.replace(
            tzinfo=None))  # Remove timezone info after shifting
        events['event_time'] = events.event_time.dt.round('L')
        events['date_key'] = events.event_time.map(
            lambda x: int(x.strftime('%Y%m%d')))
        events['event_time'] = events.event_time.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

        # Aggregate the location to a cell
        hook = AzureMsSqlDataFrameHook(
            azure_mssql_conn_id=self.sql_conn_id)

        cells = hook.read_table_dataframe(table_name="cell", schema="dim")
        cells['geometry'] = cells.wkt.map(lambda g: loads(g))
        cells = gpd.GeoDataFrame(cells)
        cells.crs = {'init': 'epsg:4326'}

        events['cell_key'] = gpd.sjoin(
            events, cells, how="left", op="within")['key']

        del cells

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.data_lake_conn_id
        )

        def find_geospatial_dim(local_path, remote_path):
            pathlib.Path(os.path.dirname(local_path)
                         ).mkdir(parents=True, exist_ok=True)

            df = hook.download_file(
                local_path, remote_path)
            df = gpd.read_file(local_path)
            df['geometry'] = df.wkt.map(loads)
            df.crs = {'init': 'epsg:4326'}

            series = gpd.sjoin(
                events.copy(), df, how="left", op="within")['key']

            del df
            os.remove(local_path)

            return series

        with ThreadPoolExecutor(max_workers=8) as executor:  #changed from 3 to 8
            city_key = executor.submit(find_geospatial_dim,
                                       self.cities_local_path, self.cities_remote_path)
            parking_district_key = executor.submit(find_geospatial_dim,
                                                   self.parking_districts_local_path, self.parking_districts_remote_path)
            pattern_area_key = executor.submit(find_geospatial_dim,
                                               self.pattern_areas_local_path, self.pattern_areas_remote_path)
            
            # New Geometry
            census_block_group_key = executor.submit(find_geospatial_dim,
                                       self.census_blocks_local_path, self.census_blocks_remote_path)
            county_key = executor.submit(find_geospatial_dim,
                                       self.counties_local_path, self.counties_remote_path)
            neighborhood_key = executor.submit(find_geospatial_dim,
                                                   self.neighborhoods_local_path, self.neighborhoods_remote_path)
            park_key = executor.submit(find_geospatial_dim,
                                               self.parks_local_path, self.parks_remote_path)
            zipcode_key = executor.submit(find_geospatial_dim,
                                               self.zipcodes_local_path, self.zipcodes_remote_path)

            events['city_key'] = city_key.result()
            events['parking_district_key'] = parking_district_key.result()
            events['pattern_area_key'] = pattern_area_key.result()

            #New Geometry
            events['census_block_group_key'] = census_block_group_key.result()
            events['county_key'] = county_key.result()
            events['neighborhood_key'] = neighborhood_key.result()
            events['park_key'] = park_key.result()
            events['zipcode_key'] = zipcode_key.result()
            


            del city_key
            del parking_district_key
            del pattern_area_key

            #New Geometry
            del census_block_group_key
            del county_key
            del neighborhood_key
            del park_key
            del zipcode_key

        del events['event_location']

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
            'cell_key',
            'census_block_group_key',
            'city_key',
            'county_key',
            'neighborhood_key',
            'park_key',
            'parking_district_key',
            'pattern_area_key',
            'zipcode_key',
            'battery_pct',
            'associated_trip',
            'seen',
            'batch'
        ]].to_csv(self.events_local_path, index=False)

        hook.upload_file(self.events_local_path, self.events_remote_path)
        hook.set_expiry(self.events_remote_path, 'RelativeToNow',
                        expire_time=(72 * 3600 * 1000))

        os.remove(self.events_local_path)

        return
