from airflow.plugins_manager import AirflowPlugin

from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator

# Defining the plugin class


class MsSqlPlugin(AirflowPlugin):
    name = "mssql_plugin"
    operators = [MsSqlOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
