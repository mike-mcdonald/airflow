from airflow.plugins_manager import AirflowPlugin
from trasportation_mobility_plugin.hooks.mobility_hook import GoogleAnalyticsHook
from trasportation_mobility_plugin.operators.mobility_reporting_to_s3_operator import GoogleAnalyticsReportingToS3Operator
from trasportation_mobility_plugin.operators.mobility_account_summaries_to_s3_operator import GoogleAnalyticsAccountSummariesToS3Operator

# Defining the plugin class


class TransportationMobilityPlugin(AirflowPlugin):
    name = "mobility_plugin"
    operators = [MobilityProviderToKafkaOperator]
    hooks = [MobilityProviderHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
