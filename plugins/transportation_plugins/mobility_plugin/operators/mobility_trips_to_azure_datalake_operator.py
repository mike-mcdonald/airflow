import json
from datetime import timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory

from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.azure_sql_dataframe_hook import AzureSqlDataFrameHook


class MobilityTripsToAzureDataLakeOperator(BaseOperator):
    """

    """

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 azure_data_lake_conn_id="azure_data_lake_default",
                 remote_path="",
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id,
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.remote_path = remote_path

    def execute(self, context):
        self.log.info(context)
        end_time = context.execution_date
        start_time = end_time - timedelta(hours=12)
        # Create the hook
        hook = MobilityProviderHook(
            mobility_provider_conn_id=self.mobility_provider_conn_id,
            mobility_provider_token_conn_id=self.mobility_provider_token_conn_id)

        # Get trips as a DataFrame
        trips = hook.get_trips(
            min_end_time=self.min_end_time, max_end_time=self.max_end_time)

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

        hook = AzureSqlDataFrameHook(
            sql_conn_id="azure_sql_server_default")
        # Break out segment hits
        cells = hook.read_dataframe(table_name="cells", schema="dim")
        cells['geometry'] = cells.wkt.map(lambda g: wkt.loads(g))
        cells = gpd.GeoDataFrame(cells)

        trips['origin'] = gpd.sjoin(
            trips.set_geometry('origin'), cells, how="left", op="intersects")['index_right']
        trips['destination'] = gpd.sjoin(
            trips.set_geometry('destination'), cells, how="left", op="intersects")['index_right']

        segments = hook.read_dataframe(table_name="segments", schema="dim")
        segments['geometry'] = segments.wkt.map(lambda g: wkt.loads(g))
        segments = gpd.GeoDataFrame(segments)

        def parse_route(trip):
            frame = trip.route
            frame['provider_id'] = trip.provider_id
            frame['vehicle_type'] = trip.vehicle_type
            frame['propulsion_type'] = ','.join(trip.propulsion_type)
            frame['date'] = frame['timestamp'].map(
                lambda dt: dt.date())
            frame['hour'] = frame['timestamp'].map(lambda dt: dt.hour)
            frame['minute'] = frame['timestamp'].map(lambda dt: dt.minute)
            return frame

        route_df = trips.apply(parse_route, axis=1)
        route_df['segment'] = gpd.sjoin(
            route_df, segments, how="left", op="intersects")['index_right']
        # Write to temp file
        with TemporaryDirectory(prefix='tmps32mds_') as tmp_dir,\
                NamedTemporaryFile(mode="wb",
                                   dir=tmp_dir,
                                   suffix='csv') as f:
            self.log.info("Dumping trips to local file {1}"
                          .format(f.name))
            f.write(trips.to_file(f.name))
            f.flush()

            # Upload to Azure Data Lake
            hook = AzureDataLakeHook(
                azure_data_lake_conn_id=self.azure_data_lake_conn_id)

            hook.upload_file(
                local_path=f.name,
                remote_path=self.remote_path)

            # Delete file
            f.delete()

        return
