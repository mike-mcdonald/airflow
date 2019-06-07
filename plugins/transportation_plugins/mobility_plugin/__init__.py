from airflow.plugins_manager import AirflowPlugin

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from transportation_plugins.mobility_plugin.operators.mobility_trips_to_sql_extract_operator import MobilityTripsToSqlExtractOperator
from transportation_plugins.mobility_plugin.operators.mobility_trips_to_sql_warehouse_operator import MobilityTripsToSqlWarehouseOperator
from transportation_plugins.mobility_plugin.operators.mobility_events_to_sql_extract_operator import MobilityEventsToSqlExtractOperator
from transportation_plugins.mobility_plugin.operators.mobility_events_to_sql_stage_operator import MobilityEventsToSqlStageOperator
from transportation_plugins.mobility_plugin.operators.mobility_routes_to_sql_warehouse_operator import MobilityEventsToSqlWarehouseOperator

# Defining the plugin class


class MobilityPlugin(AirflowPlugin):
    name = "mobility_plugin"
    operators = [MobilityTripsToSqlExtractOperator,
                 MobilityTripsToSqlWarehouseOperator,
                 MobilityEventsToSqlExtractOperator,
                 MobilityEventsToSqlStageOperator,
                 MobilityEventsToSqlWarehouseOperator]
    hooks = [MobilityProviderHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
