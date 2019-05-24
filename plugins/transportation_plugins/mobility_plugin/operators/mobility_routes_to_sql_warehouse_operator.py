import json
from datetime import datetime,  timedelta
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator

from transportation_plugins.mobility_plugin.hooks.mobility_provider_hook import MobilityProviderHook
from common_plugins.dataframe_plugin.hooks.mssql_dataframe_hook import MsSqlDataFrameHook
from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityEventsToSqlWarehouseOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = """
        MERGE fact.segmenthit AS target  
        USING (
            SELECT
            p.key AS provider_key
            ,vehicle_type
            ,pt.key AS propulsion_type_key
            ,s.key AS segment_key
            ,timestamp
            ,heading
            ,speed
            FROM etl.extract_segmenthit AS et
            LEFT JOIN dim.provider as p ON p.guid = et.provider_id
            LEFT JOIN dim.propulsion_type AS pt ON pt.name = et.plopusion_type
        ) AS source (
            provider_key
            ,vehicle_type
            ,propulsion_type_key
            ,segment_key
            ,timestamp
            ,heading
            ,speed
        )  
        ON (
            target.id = source.id
        )  
        WHEN MATCHED THEN   
            UPDATE SET last_seen = source.seen  
        WHEN NOT MATCHED THEN  
            INSERT (
                provider_key
                ,vehicle_type
                ,propulsion_type_key
                ,segment_key
                ,timestamp
                ,heading
                ,speed
            )  
            VALUES (
                source.provider_key
                ,source.vehicle_type
                ,source.propulsion_type_key
                ,source.segment_key
                ,source.timestamp
                ,source.heading
                ,source.speed
            )
        """
