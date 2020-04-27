from airflow.plugins_manager import AirflowPlugin

from parking_plugins.zendesk_plugin.hooks.zendesk_hook import ZendeskHook

# Defining the plugin class


class ZendeskPlugin(AirflowPlugin):
    name = "zendesk_plugin"
    operators = []
    hooks = [ZendeskHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
