import json
from tempfile import NamedTemporaryFile

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory

from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook

from plugins.transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook


class MobilityTripsToAzureDataLakeOperator(BaseOperator):
    """

    """

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 azure_data_lake_conn_id="azure_data_lake_default",
                 remote_path="",
                 min_end_time=None,
                 max_end_time=None,
                 * args, **kwargs):
        super(MobilityTripsToAzureDataLakeOperator,
              self).__init__(*args, **kwargs)
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id,
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.remote_path = remote_path
        self.min_end_time = min_end_time
        self.max_end_time = max_end_time

    def execute(self, context):
        # Create the hook
        hook = MobilityProviderHook(
            mobility_provider_conn_id=self.mobility_provider_conn_id,
            mobility_provider_token_conn_id=self.mobility_provider_token_conn_id)

        # Get trips
        trips = hook.get_trips(
            min_end_time=self.min_end_time, max_end_time=self.max_end_time)

        # Write to temp file
        with TemporaryDirectory(prefix='tmps32mds_') as tmp_dir,\
                NamedTemporaryFile(mode="wb",
                                   dir=tmp_dir,
                                   suffix=json) as f:
            self.log.info("Dumping trips to local file {1}"
                          .format(f.name))
            f.write(json.dumps(trips))
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
