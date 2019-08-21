import hashlib

from datetime import datetime

from pytz import timezone

from airflow.models import BaseOperator


class WazeDataLakeOperator(BaseOperator):

    template_fields = ('remote_path',)

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

    def add_default_columns(self, context, dataframe, hash_columns):
        def hash(jam):
            cols = [col for col in jam if col in hash_columns]
            s = ''
            for col in cols:
                s = s + str(col)

            return hashlib.md5(s.encode('utf-8')).hexdigest()

        dataframe['hash'] = dataframe.apply(hash, axis=1)

        dataframe['batch'] = context.get('ts_nodash')

        dataframe['seen'] = datetime.now().astimezone(timezone('US/Pacific'))
        dataframe['seen'] = dataframe.seen.dt.round('L')
        dataframe['seen'] = dataframe.seen.map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
        dataframe['seen'] = dataframe.seen.map(lambda x: x[:-3])

        return dataframe

    def execute(self, context):
        self.log.error('Called execute on base Waze DataLake Operator')
