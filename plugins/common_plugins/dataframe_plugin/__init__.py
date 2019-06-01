from airflow.plugins_manager import AirflowPlugin

from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MsSqlDataFrameHook

# Defining the plugin class


class DataFramePlugin(AirflowPlugin):
    name = "dataframe_plugin"
    operators = []
    hooks = [MsSqlDataFrameHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
