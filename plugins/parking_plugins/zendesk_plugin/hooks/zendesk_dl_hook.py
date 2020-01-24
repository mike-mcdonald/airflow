from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook as ContribHook


class ZendeskAzureDLHook(ContribHook):
    """
    Additional Azure Data Lake functionality exposed as a hook
    """

    def __init__(self, zendesk_datalake_conn_id='azure_data_lake_zendesk'):
        super().__init__(azure_data_lake_conn_id=zendesk_datalake_conn_id)

    def set_expiry(self, path, expiry_option, expire_time=None):
        self.connection.set_expiry(path, expiry_option, expire_time)

    def rm(self, path, recursive=False):
        try:
            self.connection.rm(path, recursive)
        except FileNotFoundError as err:
            self.log.debug(f"{path} not found.")

    def ls(self, path, detail=False):
        try:
            return self.connection.ls(path, detail)
        except:
            self.log.error(f'Failed to list files at {path}...')