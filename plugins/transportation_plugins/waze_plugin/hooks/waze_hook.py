import goepandas as gpd
import pandas as pd
import requests

from datetime import datetime
from shapely.geometry import Point

from airflow.hooks.base_hook import BaseHook


class WazeHook(BaseHook):
    """
    Hook for accessing waze data as a GeoDataFrame
    """

    def __init__(self, waze_conn_id='waze_default'):
        self.waze_conn_id = waze_conn_id
        self.connection = self.get_connection(self.waze_conn_id)
        self.session = requests.Session()

    def __request(self):
        res = self.session.get(self.connection.host)
        res.raise_for_status()

        return res.json()

    def get_alerts(self):
        r = self.__request()

        return gpd.GeoDataFrame(r['alerts'])

    def get_trafficjams(self):
        r = self.__request()

        return gpd.GeoDataFrame(r['jams'])

    def get_irregularities(self):
        r = self.__request()

        return gpd.GeoDataFrame(r['irregularities'])
