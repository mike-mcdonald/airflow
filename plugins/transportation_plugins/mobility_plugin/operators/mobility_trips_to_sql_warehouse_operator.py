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
from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityTripsToSqlWarehouseOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = """
        MERGE fact.trip AS target  
        USING (
            SELECT
            p.key AS provider_key 
            ,device_id
            ,vehicle_id
            ,vehicle_type
            ,pt.key AS propulsion_type_key
            ,id
            ,duration
            ,distance
            ,accuracy
            ,start_time
            ,end_time
            ,parking_verification_url
            ,standard_cost
            ,actual_cost
            ,origin
            ,destination
            FROM etl.extract_trip AS et
            LEFT JOIN dim.provider as p ON p.guid = et.provider_id
            LEFT JOIN dim.propulsion_type AS pt ON pt.name = et.plopusion_type
        ) AS source (
            provider_key
            ,device_id
            ,vehicle_id
            ,vehicle_type
            ,propulsion_type_key
            ,id
            ,duration
            ,distance
            ,accuracy
            ,start_time
            ,end_time
            ,parking_verification_url
            ,standard_cost
            ,actual_cost
            ,origin
            ,destination
            ,seen
        )  
        ON (
            target.id = source.id
        )  
        WHEN MATCHED THEN   
            UPDATE SET last_seen = source.seen  
        WHEN NOT MATCHED THEN  
            INSERT (
                provider_key
                ,device_id
                ,vehicle_id
                ,vehicle_type
                ,propulsion_type_key
                ,id
                ,duration
                ,distance
                ,accuracy
                ,start_time
                ,end_time
                ,parking_verification_url
                ,standard_cost
                ,actual_cost
                ,origin
                ,destination
                ,first_seen
            )  
            VALUES (
                 source.provider_key
                ,source.device_id
                ,source.vehicle_id
                ,source.vehicle_type
                ,source.propulsion_type_key
                ,source.id
                ,source.duration
                ,source.distance
                ,source.accuracy
                ,source.start_time
                ,source.end_time
                ,source.parking_verification_url
                ,source.standard_cost
                ,source.actual_cost
                ,source.origin
                ,source.destination
                ,source.seen
            )
        """
