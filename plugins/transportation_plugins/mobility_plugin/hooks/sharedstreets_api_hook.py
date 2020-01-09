import time

from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class SharedStreetsAPIHook(BaseHook):
    def __init__(self,
                 max_retries=2,
                 shst_conn_id="shst_api_default"):
        self.max_retries = max_retries

        try:
            self.connection = self.get_connection(shst_conn_id)
        except AirflowException as err:
            self.log.error(
                f"Failed to find connection for SharedStreets API: {shst_conn_id}. Error: {err}")
            raise err

        self.session = requests.Session()

    def _request(self, url, data=None):
        """
        Internal helper for sending requests.

        Returns payload(s).
        """
        retries = 0
        res = None

        while res is None:
            try:
                res = self.session.post(url, data=data)
                res.raise_for_status()
            except Exception as err:
                res = None
                retries = retries + 1
                f retries > self.max_retries:
                    raise AirflowException(
                        f"Unable to retrieve response from {url} after {self.max_retries}.  Aborting...")

                self.log.warning(
                    f"Error while retrieving {url}: {err}.  Retrying in {10 * retries} seconds... (retry {retries}/{self.max_retries})")
                res = None
                time.sleep(10 * retries)

        return res

    def match(self, geometry_type, profile, fc):
        """

        """

        # make the request(s)
        res = self._request(
            f'{self.connection.host}/{geometry_type}/{profile}', fc)

        try:
            results = res.json()
        }

        return results
