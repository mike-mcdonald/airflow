from airflow.plugins_manager import AirflowPlugin

from transportation_plugins.mobility_plugin.hooks.gbfs_hook import GBFSFeedHook
from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from transportation_plugins.mobility_plugin.hooks.sharedstreets_api_hook import SharedStreetsAPIHook
from transportation_plugins.mobility_plugin.operators.mobility_trips_to_sql_extract_operator import MobilityTripsToSqlExtractOperator
from transportation_plugins.mobility_plugin.operators.mobility_events_to_sql_extract_operator import MobilityEventsToSqlExtractOperator
from transportation_plugins.mobility_plugin.operators.mobility_provider_sync_operator import MobilityProviderSyncOperator
from transportation_plugins.mobility_plugin.operators.mobility_vehicle_sync_operator import MobilityVehicleSyncOperator
from transportation_plugins.mobility_plugin.operators.mobility_fleet_to_sql_extract_operator import MobilityFleetToSqlExtractOperator
# Defining the plugin class


class MobilityPlugin(AirflowPlugin):
    name = "mobility_plugin"
    operators = [MobilityTripsToSqlExtractOperator,
                 MobilityEventsToSqlExtractOperator,
                 MobilityProviderSyncOperator,
                 MobilityVehicleSyncOperator,
                 MobilityFleetToSqlExtractOperator]
    hooks = [GBFSFeedHook, MobilityProviderHook, SharedStreetsAPIHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
