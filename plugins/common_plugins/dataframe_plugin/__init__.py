from airflow.plugins_manager import AirflowPlugin

from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MSSqlDataFrameHook

# Defining the plugin class


class DataFramePlugin(AirflowPlugin):
    name = "dataframe_plugin"
    operators = []
    hooks = [MSSqlDataFrameHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
