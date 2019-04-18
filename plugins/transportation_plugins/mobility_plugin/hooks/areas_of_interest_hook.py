import geopandas as gpd

from airflow.hooks.base_hook import BaseHook


class AreasOfInterestHook(BaseHook):
    def __init__(self,
                 no_parking_conn_id="no_parking_zones_default",
                 no_riding_conn_id="no_riding_zones_default",
                 pattern_areas_conn_id="pattern_areas_default",
                 parks_conn_id="parks_default",
                 bins_conn_id="bins_default"):
        self.no_parking_connection = self.get_connection(no_parking_conn_id)
        self.no_riding_connection = self.get_connection(no_riding_conn_id)
        self.pattern_areas_connection = self.get_connection(
            pattern_areas_conn_id)
        self.parks_connection = self.get_connection(parks_conn_id)
        self.bins_connection = self.get_connection(bins_conn_id)

    def get_no_parking_zones(self):
        return gpd.read_file(self.no_parking_connection.host)

    def get_no_riding_zones(self):
        return gpd.read_file(self.no_riding_connection.host)

    def get_pattern_areas(self):
        return gpd.read_file(self.pattern_areas_connection.host)

    def get_parks(self):
        return gpd.read_file(self.parks_connection.host)

    def get_bins(self):
        return gpd.read_file(self.bins_connection.host)
