import pathlib
from datetime import datetime

from pytz import timezone
from shapely.geometry import Point
from shapely.wkt import dumps

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook
from transportation_plugins.waze_plugin.hooks.waze_hook import WazeHook


class WazeTrafficJamsToDataLakeOperator(BaseOperator):

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

        jams['pubMillis'] = jams.pubMillis.map(
            lambda x: datetime.fromtimestamp(x / 1000).astimezone(timezone('US/Pacific')))
        jams['pubMillis'] = jams.pubMillis.dt.round('L')
        jams['pubMillis'] = jams.pubMillis.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
        jams['pubMillis'] = jams.pubMillis.map(lambda x: x[:-3])

        jams['line'] = jams.line.map(lambda x: [x['x'], x['y']])
        jams['line'] = jams.line.apply(Point)
        jams['line'] = jams.line.apply(dumps)

        pathlib.Path(os.path.dirname(self.local_path)
                     ).mkdir(parents=True, exist_ok=True)

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.azure_data_lake_conn_id)

        hook.upload_file(self.local_path, self.remote_path)

        os.remove(self.local_path)
