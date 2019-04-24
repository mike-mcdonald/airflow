from airflow.plugins_manager import AirflowPlugin

from common_plugins.calendar_plugin.hooks.create_calendar_hook import CreateCalendarHook
from common_plugins.calendar_plugin.operators.calendar_to_sql_table import CalendarToSqlTableOperator

# Defining the plugin class


class CalendarPlugin(AirflowPlugin):
    name = "calendar_plugin"
    operators = [CalendarToSqlTableOperator]
    hooks = [CreateCalendarHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
