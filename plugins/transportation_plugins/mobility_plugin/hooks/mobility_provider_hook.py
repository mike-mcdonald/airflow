from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from airflow.hooks.base_hook import BaseHook


class MobilityProviderHook(BaseHook):
    def __init__(self,
                 version="0.3.0",
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None):
        try:
            self.connection = self.get_connection(mobility_provider_conn_id)
        except:
            self.log.error(
                f"Failed to find connection for mobility provider: {mobility_provider_conn_id}")

        self.session = requests.Session()

        if mobility_provider_token_conn_id is not None:
            try:
                self.token_connection = self.get_connection(
                    mobility_provider_token_conn_id)
            except:
                self.log.error(
                    f"Failed to find token connection for mobility provider: {mobility_provider_token_conn_id}")

            grant_type = self.token_connection.extra_dejson["grant_type"] or "client_credentials"
            login_key = self.token_connection.extra_dejson["login_key"] or "client_id"
            password_key = self.token_connection.extra_dejson["password_key"] or "client_secret"
            auth_type = self.token_connection.extra_dejson["auth_type"] or "Bearer"
            token_key = self.token_connection.extra_dejson["token_key"] or "access_token"

            payload = {
                f"{login_key}": self.token_connection.login,
                f"{password_key}": self.token_connection.password,
                "grant_type": grant_type
            }
            r = requests.post(self.token_connection.host, params=payload)
            token = r.json()[token_key]
            self.session.headers.update(
                {"Authorization": f"{auth_type} {token}"})

        self.session.headers.update(self.connection.extra_dejson["headers"])

    def _date_format(self, dt):
        return int(dt.timestamp()) if isinstance(dt, datetime) else int(dt)

    def __request(self, url, endpoint, params, results=[]):
        """
        Internal helper for sending requests.

        Returns payload(s).
        """
        res = self.session.get(url, params=params)

        if res.status_code is not 200:
            self.log.warning(res)
            return results

        page = res.json()

        if page["data"] is not None:
            results.append(page["data"][endpoint])

        if "links" in page:
            next_page = page["links"].get("next")
            if next_page is not None:
                self.__request(url=next_page, endpoint=endpoint,
                               params=None, results=results)

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
        if min_end_time is not None:
            min_end_time = self._date_format(min_end_time)
        if max_end_time is not None:
            max_end_time = self._date_format(max_end_time)

        # gather all the params togethers
        params = {
            **dict(device_id=device_id, vehicle_id=vehicle_id, min_end_time=min_end_time, max_end_time=max_end_time),
        }

        # make the request(s)
        trips = self.__request(
            urljoin(self.connection.host, "trips"), "trips", params)

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
        if providers is None:
            providers = self.providers

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params together
        params = {
            **dict(start_time=start_time, end_time=end_time)
        }

        # make the request(s)
        status_changes = self.__request(
            urljoin(self.connection.host, "status_changes"), "status_changes", params)

        return pd.DataFrame.from_records(status_changes)
