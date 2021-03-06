import time

from datetime import datetime
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class MobilityProviderHook(BaseHook):
    def __init__(self,
                 version="0.3",
                 max_retries=3,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id="mobility_provider_token_default"):
        self.version = version
        self.max_retries = max_retries

        try:
            self.connection = self.get_connection(mobility_provider_conn_id)
        except AirflowException as err:
            self.log.error(
                f"Failed to find connection for mobility provider: {mobility_provider_conn_id}. Error: {err}")
            raise

        self.session = requests.Session()

        try:
            self.token_connection = self.get_connection(
                mobility_provider_token_conn_id)
        except AirflowException as err:
            self.log.warning(
                f"Failed to find token connection for mobility provider: {mobility_provider_token_conn_id}. Error: {err}")

        if hasattr(self, 'token_connection'):
            try:
                auth_type = self.token_connection.extra_dejson[
                    "auth_type"] if "auth_type" in self.token_connection.extra_dejson else "Bearer"
                token_key = self.token_connection.extra_dejson[
                    "token_key"] if "token_key" in self.token_connection.extra_dejson else "access_token"

                payload = self.connection.extra_dejson["auth_payload"]
                res = requests.post(self.token_connection.host,
                                    data=payload)
                self.log.debug(
                    f"Received token endpoint response from: {mobility_provider_token_conn_id}, response: {res.json()} ")
                token = res.json()[token_key]

                self.session.headers.update(
                    {"Authorization": f"{auth_type} {token}"})
            except Exception as err:
                self.log.error(
                    f"Failed to authorize token connection for mobility provider: {mobility_provider_token_conn_id}. Error: {err}")

        if hasattr(self.connection, 'extra_dejson'):
            if "headers" in self.connection.extra_dejson:
                self.session.headers.update(
                    self.connection.extra_dejson["headers"])

        self.session.headers.update({
            "Accept": f"application/vnd.mds.provider+json;version={self.version}"
        })

    def _date_format(self, dt):
        return int(dt.timestamp()) * 1000

    def _request(self, url, payload_key, params=None, results=[]):
        """
        Internal helper for sending requests.

        Returns payload(s).
        """

        while url is not None:
            retries = 0
            res = None

            # for subsequent pages, update query params
            parsed = urlparse(url)
            params.update(parse_qs(parsed.query))

            # clear params from url query string
            url = parsed._replace(query=None).geturl()

            while res is None:
                try:
                    res = self.session.get(url, params=params)
                    res.raise_for_status()
                except requests.HTTPError as err:
                    if res.status_code == 404:
                        # This is supposed to mean there are no objects for the requested time period
                        # TODO: Hides an actual change of address for the API or other more serious issues
                        return results

                    retries = retries + 1
                    if retries > self.max_retries:
                        raise AirflowException(
                            f"Unable to retrieve response from {url} after {self.max_retries}.  Aborting...")

                    self.log.warning(
                        f"Error while retrieving {url}: {err}.  Retrying in {10 * retries} seconds... (retry {retries}/{self.max_retries})")
                    res = None
                    time.sleep(10 * retries)

            self.log.debug(f"Received response from {url}")

            if "Content-Type" in res.headers:
                cts = res.headers["Content-Type"].split(";")
                if "application/vnd.mds.provider+json" not in cts:
                    self.log.warning(
                        f"Incorrect content-type returned: {res.headers['Content-Type']}")
                cts = cts[1:]
                for ct in cts:
                    if ct.strip().startswith("charset"):
                        pass
                    if not ct.strip().startswith(f"version={self.version}"):
                        self.log.warning(
                            f"Incorrect content-type returned: {res.headers['Content-Type']}")
            else:
                self.log.error(f"Missing {self.version} content-type header.")

            page = res.json()

            if page["data"] is not None:
                results.extend(page["data"][payload_key])

            if "links" in page:
                next_page = page["links"].get("next")
                if "rate_limit" in self.connection.extra_dejson:
                    time.sleep(int(self.connection.extra_dejson["rate_limit"]))
                url = next_page
            else:
                url = None

        return results

    def get_trips(self, device_id=None, vehicle_id=None, min_end_time=None, max_end_time=None):
        """
        Request Trips data. Returns a DataFrame of trips payload(s).

        Supported keyword args:

            - `device_id`: Filters for trips taken by the given device.

            - `vehicle_id`: Filters for trips taken by the given vehicle.

            - `min_end_time`: Filters for trips where `end_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `max_end_time`: Filters for trips where `end_time` occurs before the given time
                          Should be a datetime object or numeric representation of UNIX seconds
        """
        # convert datetimes to querystring friendly format
        self.log.debug(
            f"Retrieving trips for period {min_end_time} to {max_end_time}")

        if min_end_time is not None:
            min_end_time = self._date_format(min_end_time)
        if max_end_time is not None:
            max_end_time = self._date_format(max_end_time)

        # gather all the params togethers
        params = dict(min_end_time=min_end_time, max_end_time=max_end_time)

        # make the request(s)
        trips = self._request(
            self.connection.host.replace(":endpoint", "trips"), "trips", params)

        return pd.DataFrame.from_records(trips)

    def get_events(self, start_time=None, end_time=None):
        """
        Request Status Changes data. Returns a DataFrame of status_changes payload(s)

        Supported keyword args:

            - `start_time`: Filters for status changes where `event_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for status changes where `event_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

        """

        self.log.debug(
            f"Retrieving events for period {start_time} to {end_time}")

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params together
        params = dict(start_time=start_time, end_time=end_time)

        # make the request(s)
        status_changes = self._request(
            self.connection.host.replace(":endpoint", "status_changes"), "status_changes", params)

        return pd.DataFrame.from_records(status_changes)
