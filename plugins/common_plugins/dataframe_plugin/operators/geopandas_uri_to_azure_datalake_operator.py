import hashlib
import pathlib
import os

import geopandas as gpd

from shapely.wkt import dumps

from common_plugins.azure_plugin.hooks.azure_data_lake_hook import AzureDataLakeHook


from airflow.models import BaseOperator


class GeoPandasUriToAzureDataLakeOperator(BaseOperator):
    template_fields = ('uri', 'local_path', 'remote_path')

    def __init__(self,
                 uri=None,
                 local_path=None,
                 azure_data_lake_conn_id='azure_data_lake_default',
                 remote_path=None,
                 rename={},
                 columns=[],
                 index_label='key',
                 df_crs={'init': 'epsg:4326'},
                 area_epsg=3857,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.local_path = local_path
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.remote_path = remote_path
        self.rename = rename
        self.columns = columns
        self.index_label = index_label
        self.df_crs = df_crs
        self.area_epsg = area_epsg

    def execute(self, context):
        df = gpd.read_file(self.uri)
        df.crs = self.df_crs

        df['wkt'] = df.geometry.map(dumps)
        df['hash'] = df.wkt.map(lambda x: hashlib.md5(
            x.encode('utf-8')).hexdigest())

        df['center'] = df.geometry.map(lambda x: x.centroid)
        df['center_x'] = df.center.map(lambda x: x.x)
        df['center_y'] = df.center.map(lambda x: x.y)

        df = df.to_crs(epsg=self.area_espg)
        df['area'] = df.geometry.map(lambda x: x.area)

        pathlib.Path(os.path.dirname(self.local_path)
                     ).mkdir(parents=True, exist_ok=True)

        df = df.rename(index=str, columns=self.rename)

        df[self.columns].to_csv(self.local_path, index_label=self.index_label)

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.azure_data_lake_conn_id)

        hook.upload_file(self.local_path, self.remote_path)

        os.remove(self.local_path)
