
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook


class AzureDataLakeRemoveOperator(BaseOperator):

    template_fields = ('remote_path',)

    # @apply_defaults
    def __init__(
            self, azure_data_lake_conn_id='azure_data_lake_default', remote_path=None, recursive=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.remote_path = remote_path
        self.recursive = recursive

    def execute(self, context):
        self.log.info('Deleting: %s', self.remote_path)
        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.azure_data_lake_conn_id)

        hook.rm(self.remote_path, recursive=self.recursive)
