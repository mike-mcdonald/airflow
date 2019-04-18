import json
from datetime import timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory

from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook

from plugins.transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from plugins.transportation_plugins.mobility_plugin.hooks.areas_of_interest_hook import AreasOfInterestHook


class MobilityTripsToTempFileOperator(BaseOperator):
    """

    """

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 azure_data_lake_conn_id="azure_data_lake_default",
                 temp_path="",
                 * args, **kwargs):
        super(MobilityTripsToTempFileOperator,
              self).__init__(*args, **kwargs)
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id,
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.temp_path = temp_path
        self.max_end_time = kwargs["execution_time"]
        self.min_end_time = self.max_end_time - timedelta(hours=12)

    def execute(self, context):
        # Create the hook
        hook = MobilityProviderHook(
            mobility_provider_conn_id=self.mobility_provider_conn_id,
            mobility_provider_token_conn_id=self.mobility_provider_token_conn_id)

        # Get trips
        trips = hook.get_trips(
            min_end_time=self.min_end_time, max_end_time=self.max_end_time)

        # Let's get our datasets to enrich this data
        areas_hook = AreasOfInterestHook()

        bins = areas_hook.get_bins()
        pattern_areas = areas_hook.get_pattern_areas()
        no_parking_zones = areas_hook.get_no_parking_zones()
        no_riding_zones = areas_hook.get_no_riding_zones()

        # Convert trips to a GeoDataFrame that

        # Write to temp file
        with open(self.temp_path, "wb") as f:
            self.log.info("Dumping trips to local file {1}"
                          .format(f.name))
            f.write(json.dumps(trips))
            f.flush()

        return
