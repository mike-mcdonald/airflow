import json
import time

from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class SharedStreetsAPIHook(BaseHook):
    def __init__(self,
                 max_tries=3,
                 shst_api_conn_id="shst_api_default"):
        self.max_tries = max_tries

        try:
            self.connection = self.get_connection(shst_api_conn_id)
        except AirflowException as err:
            self.log.error(
                f"Failed to find connection: {shst_api_conn_id}.")
            raise err

        self.session = requests.Session()

        self.session.headers.update({"Content-Type": "application/json"})
        self.session.headers.update({"Accept": "application/json"})

    def _request(self, url, data=None):
        """
        Internal helper for sending requests.
        """
        tries = 1
        res = None

        self.log.debug(f"Making request to: {url}")

        while res is None:
            try:
                res = self.session.post(url, data=data)
                res.raise_for_status()
            except Exception as err:
                tries = tries + 1
                if tries >= self.max_tries:
                    raise AirflowException(
                        f"Unable to retrieve response from {url} after {self.max_tries}.  Aborting...")

                self.log.warning(
                    f"Error while retrieving {url}: {err}.  Retrying in {10 * tries} seconds... (retry {tries}/{self.max_tries})")
                res = None
                time.sleep(10 * tries)

        self.log.debug(f"Received response from {url}")

        results = res.json()

        return results

    def match(self, geometry_type, profile, fc):
        """
        Request shst api transformation on GeoJSON FEatureCollection 'fc'.

        Returns a GeoJSON FeatureCollection as a JSON dictionary.

        """

        # make the request(s)
        return self._request(
            self.connection.host.replace(":geometry_type", geometry_type).replace(":profile", profile), data=json.dumps(fc))
