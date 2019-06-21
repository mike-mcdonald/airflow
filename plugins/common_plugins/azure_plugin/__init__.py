from airflow.plugins_manager import AirflowPlugin

from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook

# Defining the plugin class


class AzurePlugin(AirflowPlugin):
    name = "azure_plugin"
    operators = []
    hooks = [AzureDataLakeHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
