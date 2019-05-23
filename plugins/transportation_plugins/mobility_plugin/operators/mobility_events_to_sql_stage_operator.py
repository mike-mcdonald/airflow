import json
from datetime import datetime,  timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator

from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityEventsToSqlStageOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = """
        INSERT INTO etl.stage_state (
            provider_key
            ,vehicle_id
            ,vehicle_type
            ,propulsion_type_key
            ,start_state
            ,start_event
            ,start_time
            ,start_cell_key
            ,start_battery_pct
            ,end_state
            ,end_event
            ,end_time
            ,end_cell_key
            ,end_battery_pct
        )
        SELECT
        ,provider_id
        ,provider_name
        ,device_id
        ,vehicle_id
        ,vehicle_type
        ,propulsion_type
        ,event_type
        ,event_type_reason
        ,event_time
        ,event_location
        ,battery_pct
        ,associated_trip

        """
