import time

from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class GBFSFeedHook(BaseHook):
    def __init__(self,
                 max_retries=2,
                 gbfs_feed_conn_id="gbfs_feed_default"):
        self.max_retries = max_retries

        try:
            self.connection = self.get_connection(gbfs_feed_conn_id)
        except AirflowException as err:
            self.log.error(
                f"Failed to find connection for mobility provider: {gbfs_feed_conn_id}. Error: {err}")
            raise err

        self.session = requests.Session()

    def _request(self, url, payload_key, results=[]):
        """
        Internal helper for sending requests.

        Returns payload(s).
        """
        retries = 0
        res = None

        self.log.debug(f"Making request to: {url}")

        while res is None:
            try:
                res = self.session.get(url)
                res.raise_for_status()
            except Exception as err:
                retries = retries + 1
                if retries > self.max_retries:
                    raise AirflowException(
                        f"Unable to retrieve response from {url} after {self.max_retries}.  Aborting...")

                self.log.warning(
                    f"Error while retrieving {url}: {err}.  Retrying in {10 * retries} seconds... (retry {retries}/{self.max_retries})")
                res = None
                time.sleep(10 * retries)

        self.log.debug(f"Received response from {url}")

        page = res.json()

        if page["data"] is not None and payload_key in page["data"]:
            results.extend(page["data"][payload_key])

        return results

    def get_free_bikes(self):
        """
        Request free_bike_status.json data. Returns a DataFrame of bike payload(s).

        """

        # make the request(s)
        bikes = self._request(
            self.connection.host.replace(":endpoint", "free_bike_status"), "bikes")

        return pd.DataFrame.from_records(bikes)
