from airflow.plugins_manager import AirflowPlugin

from transportation_plugins.waze_plugin.hooks.waze_hook import WazeHook

from transportation_plugins.waze_plugin.operators.waze_alerts_to_datalake_operator import WazeAlertsToDataLakeOperator
from transportation_plugins.waze_plugin.operators.waze_trafficjams_to_datalake_operator import WazeTrafficJamsToDataLakeOperator
# Defining the plugin class


class WazePlugin(AirflowPlugin):
    name = "waze_plugin"
    operators = [WazeAlertsToDataLakeOperator,
                 WazeTrafficJamsToDataLakeOperator]
    hooks = [WazeHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
