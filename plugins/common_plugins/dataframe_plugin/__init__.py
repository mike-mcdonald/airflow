from airflow.plugins_manager import AirflowPlugin

from common_plugins.dataframe_plugin.hooks.azure_sql_dataframe_hook import AzureSqlDataFrameHook

# Defining the plugin class


class DataFramePlugin(AirflowPlugin):
    name = "dataframe_plugin"
    operators = []
    hooks = [AzureSqlDataFrameHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
