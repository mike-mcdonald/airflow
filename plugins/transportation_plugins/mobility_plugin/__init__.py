from airflow.plugins_manager import AirflowPlugin

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from transportation_plugins.mobility_plugin.operators.mobility_trips_to_sql_extract_operator import MobilityTripsToSqlExtractOperator
from transportation_plugins.mobility_plugin.operators.mobility_trips_to_sql_warehouse_operator import MobilityTripsToSqlWarehouseOperator
from transportation_plugins.mobility_plugin.operators.mobility_events_to_sql_extract_operator import MobilityEventsToSqlExtractOperator
from transportation_plugins.mobility_plugin.operators.mobility_events_to_sql_stage_operator import MobilityEventsToSqlStageOperator
from transportation_plugins.mobility_plugin.operators.mobility_states_to_sql_warehouse_operator import MobilityStatesToSqlWarehouseOperator
from transportation_plugins.mobility_plugin.operators.mobility_routes_to_sql_warehouse_operator import MobilityEventsToSqlWarehouseOperator
from transportation_plugins.mobility_plugin.operators.mobility_provider_sync_operator import MobilityProviderSyncOperator
from transportation_plugins.mobility_plugin.operators.mobility_vehicle_sync_operator import MobilityVehicleSyncOperator
from transportation_plugins.mobility_plugin.operators.mobility_fleet_to_sql_extract_operator import MobilityFleetToSqlExtractOperator
from transportation_plugins.mobility_plugin.operators.mobility_fleet_to_sql_stage_operator import MobilityFleetToSqlStageOperator
from transportation_plugins.mobility_plugin.operators.mobility_fleet_to_sql_warehouse_operator import MobilityFleetToSqlWarehouseOperator
# Defining the plugin class


class MobilityPlugin(AirflowPlugin):
    name = "mobility_plugin"
    operators = [MobilityTripsToSqlExtractOperator,
                 MobilityTripsToSqlWarehouseOperator,
                 MobilityEventsToSqlExtractOperator,
                 MobilityEventsToSqlStageOperator,
                 MobilityStatesToSqlWarehouseOperator,
                 MobilityProviderSyncOperator,
                 MobilityVehicleSyncOperator,
                 MobilityFleetToSqlExtractOperator,
                 MobilityFleetToSqlStageOperator,
                 MobilityFleetToSqlWarehouseOperator]
    hooks = [MobilityProviderHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
