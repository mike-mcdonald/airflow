import pathlib
import os
from datetime import datetime

from pytz import timezone
from shapely.geometry import Point
from shapely.wkt import dumps

from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook
from transportation_plugins.waze_plugin.hooks.waze_hook import WazeHook
from transportation_plugins.waze_plugin.operators.waze_datalake_operator import WazeDataLakeOperator


class WazeAlertsToDataLakeOperator(WazeDataLakeOperator):

    template_fields = ('local_path', 'remote_path',)

    # @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(WazeAlertsToDataLakeOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = WazeHook(waze_conn_id=self.waze_conn_id)

        alerts = hook.get_alerts()

        alerts = self.add_default_columns(
            context, alerts, ['uuid', 'pubMillis'])

        alerts['pubMillis'] = alerts.pubMillis.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
        alerts['pubMillis'] = alerts.pubMillis.dt.round('L')
        alerts['pubMillis'] = alerts.pubMillis.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
        alerts['pubMillis'] = alerts.pubMillis.map(lambda x: x[:-3])

        alerts['location'] = alerts.location.map(lambda x: [x['x'], x['y']])
        alerts['location'] = alerts.location.apply(Point)
        alerts['location'] = alerts.location.apply(dumps)

        pathlib.Path(os.path.dirname(self.local_path)
                     ).mkdir(parents=True, exist_ok=True)

        alerts.to_csv(self.local_path, index=False)

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.azure_data_lake_conn_id)

        hook.upload_file(self.local_path, self.remote_path)

        os.remove(self.local_path)
