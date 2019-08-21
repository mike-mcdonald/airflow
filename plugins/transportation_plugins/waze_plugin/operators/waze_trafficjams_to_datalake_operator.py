import hashlib
import pathlib
import os
from datetime import datetime

from pytz import timezone
from shapely.geometry import LineString
from shapely.wkt import dumps

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook
from transportation_plugins.waze_plugin.hooks.waze_hook import WazeHook
from transportation_plugins.waze_plugin.operators.waze_datalake_operator import WazeDataLakeOperator


class WazeTrafficJamsToDataLakeOperator(WazeDataLakeOperator):

    template_fields = ('local_path', 'remote_path',)

    # @apply_defaults
    def __init__(self,
                 waze_conn_id='waze_default',
                 azure_data_lake_conn_id='azure_data_lake_default',
                 local_path=None,
                 remote_path=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.waze_conn_id = waze_conn_id
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.local_path = local_path
        self.remote_path = remote_path

    def execute(self, context):
        hook = WazeHook(waze_conn_id=self.waze_conn_id)

        jams = hook.get_trafficjams()

        jams = self.add_default_columns(
            context, jams, ['uuid', 'pubMillis'])

        jams['pubMillis'] = jams.pubMillis.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
        jams['pubMillis'] = jams.pubMillis.dt.round('L')
        jams['pubMillis'] = jams.pubMillis.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
        jams['pubMillis'] = jams.pubMillis.map(lambda x: x[:-3])

        jams['line'] = jams.line.map(lambda l: [[x['x'], x['y']] for x in l])
        jams['line'] = jams.line.apply(LineString)
        jams['line'] = jams.line.apply(dumps)

        pathlib.Path(os.path.dirname(self.local_path)
                     ).mkdir(parents=True, exist_ok=True)

        jams.to_csv(self.local_path)

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.azure_data_lake_conn_id)

        hook.upload_file(self.local_path, self.remote_path)

        os.remove(self.local_path)
