from airflow.plugins_manager import AirflowPlugin

from common_plugins.dataframe_plugin.hooks.sql_dataframe_hook import SqlDataFrameHook
from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MsSqlDataFrameHook
from common_plugins.dataframe_plugin.hooks.azure_mssql_dataframe_hook import AzureMsSqlDataFrameHook
from common_plugins.dataframe_plugin.hooks.pgsql_dataframe_hook import PgSqlDataFrameHook

from common_plugins.dataframe_plugin.operators.geopandas_uri_to_azure_datalake_operator import GeoPandasUriToAzureDataLakeOperator

# Defining the plugin class


class DataFramePlugin(AirflowPlugin):
    name = "dataframe_plugin"
    operators = [GeoPandasUriToAzureDataLakeOperator]
    hooks = [SqlDataFrameHook,
             MsSqlDataFrameHook,
             AzureMsSqlDataFrameHook,
             PgSqlDataFrameHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
