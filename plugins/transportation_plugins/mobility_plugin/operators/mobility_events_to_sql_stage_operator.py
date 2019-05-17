import json
from datetime import datetime,  timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MSSqlDataFrameHook


class MobilityEventsToSqlStageOperator(BaseOperator):
    """

    """

    def __init__(self,
                 sql_conn_id="sql_server_default",
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.sql = """
        SELECT
        """

    def execute(self, context):
        hook = MsSqlHook(
            mssql_conn_id=self.sql_conn_id
        )

        return
