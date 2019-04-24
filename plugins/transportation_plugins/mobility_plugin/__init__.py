from airflow.plugins_manager import AirflowPlugin

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from transportation_plugins.mobility_plugin.hooks.areas_of_interest_hook import AreasOfInterestHook
from transportation_plugins.mobility_plugin.operators.mobility_trips_to_azure_datalake_operator import MobilityTripsToAzureDataLakeOperator

# Defining the plugin class


class MobilityPlugin(AirflowPlugin):
    name = "mobility_plugin"
    operators = [MobilityTripsToAzureDataLakeOperator]
    hooks = [MobilityProviderHook, AreasOfInterestHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
