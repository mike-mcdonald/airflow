from airflow.plugins_manager import AirflowPlugin

from plugins.transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from plugins.transportation_plugins.mobility_plugin.operators.mobility_trips_to_azure_datalake_operator import MobilityTripsToAzureDataLakeOperator

# Defining the plugin class


class MobilityPlugin(AirflowPlugin):
    name = "mobility_plugin"
    operators = [MobilityTripsToAzureDataLakeOperator]
    hooks = [MobilityProviderHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
