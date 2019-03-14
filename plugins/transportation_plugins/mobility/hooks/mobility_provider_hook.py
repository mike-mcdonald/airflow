from datetime import datetime
import time
import json
from requests import Session
from requests_oauthlib import OAuth2Session

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator

# Will show up under airflow.hooks.mobility_plugin.MobilityProviderHook


class MobilityProviderHook(BaseHook):
    def __init__(self,
                 mobility_provider_conn_id='mobility_provider_default',
                 mobility_provider_token_conn_id='mobility_provider_token_default'):
        self.mobility_provider_conn_id = mobility_provider_conn_id
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.connection = self.get_connection(mobility_provider_conn_id)
        self.token_connection = self.get_connection(
            mobility_provider_token_conn_id)
        if mobility_provider_token_conn_id is not None:
            self.session = OAuth2Session(client_id=self.token_connection.login,
                                         scope=self.token_connection.extra_dejson["scope"])
        else:
            self.session = Session()

    def __request(self, url, params):
        """
        Internal helper for sending requests.

        Returns a dict of provider => payload(s).
        """
        def __describe(res):
            """
            Prints details about the given response.
            """
            print(f"Requested {res.url}, Response Code: {res.status_code}")
            print("Response Headers:")
            for k, v in res.headers.items():
                print(f"{k}: {v}")

            if r.status_code is not 200:
                print(r.text)

        def __has_data(page):
            """
            Checks if this :page: has a "data" property with a non-empty payload
            """
            data = page["data"] if "data" in page else {"__payload__": []}
            payload = data[endpoint] if endpoint in data else []
            print(f"Got payload with {len(payload)} {endpoint}")
            return len(payload) > 0

        def __next_url(page):
            """
            Gets the next URL or None from :page:
            """
            return page["links"].get("next") if "links" in page else None

        # create a request url for each provider
        urls = [self._build_url(p, endpoint) for p in providers]

        # keyed by provider
        results = {}

        for i in range(len(providers)):
            provider, url = providers[i], urls[i]

            # establish an authenticated session
            session = self._auth_session(provider)

            # get the initial page of data
            r = session.get(url, params=params)

            if r.status_code is not 200:
                __describe(r)
                continue

            this_page = r.json()

            # track the list of pages per provider
            results[provider] = [this_page] if __has_data(this_page) else []

            # get subsequent pages of data
            next_url = __next_url(this_page)
            while paging and next_url:
                r = session.get(next_url)

                if r.status_code is not 200:
                    __describe(r)
                    break

                this_page = r.json()

                if __has_data(this_page):
                    results[provider].append(this_page)

                next_url = __next_url(this_page)

                if next_url and rate_limit:
                    time.sleep(rate_limit)

        return results

    def get_trips(self, device_id, vehicle_id, min_end_time, max_end_time):
        """
        Request Trips data. Returns a dict of provider => list of trips payload(s).

        Supported keyword args:

            - `providers`: One or more Providers to issue this request to.
                           The default is to issue the request to all Providers.

            - `device_id`: Filters for trips taken by the given device.

            - `vehicle_id`: Filters for trips taken by the given vehicle.

            - `start_time`: Filters for trips where `start_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for trips where `end_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

            - `bbox`: Filters for trips where and point within `route` is within defined bounding-box.
                      The order is defined as: southwest longitude, southwest latitude, 
                      northeast longitude, northeast latitude (separated by commas).

                      e.g.

                      bbox=-122.4183,37.7758,-122.4120,37.7858

            - `paging`: True (default) to follow paging and request all available data.
                        False to request only the first page.

            - `rate_limit`: Number of seconds of delay to insert between paging requests.
        """
        if providers is None:
            providers = self.providers

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params togethers
        params = {
            **dict(device_id=device_id, vehicle_id=vehicle_id, start_time=start_time, end_time=end_time),
            **kwargs
        }

        # make the request(s)
        trips = self._request(providers, mds.TRIPS, params, paging, rate_limit)

        return trips

    def get_events(self, start_time=None, end_time=None):
        """
        Request Status Changes data. Returns a dict of provider => list of status_changes payload(s)

        Supported keyword args:

            - `providers`: One or more Providers to issue this request to.
                           The default is to issue the request to all Providers.

            - `start_time`: Filters for status changes where `event_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for status changes where `event_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

            - `bbox`: Filters for status changes where `event_location` is within defined bounding-box.
                      The order is defined as: southwest longitude, southwest latitude, 
                      northeast longitude, northeast latitude (separated by commas).

                      e.g.

                      bbox=-122.4183,37.7758,-122.4120,37.7858

            - `paging`: True (default) to follow paging and request all available data.
                        False to request only the first page.

            - `rate_limit`: Number of seconds of delay to insert between paging requests.
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
            **dict(start_time=start_time, end_time=end_time, bbox=bbox),
            **kwargs
        }

        # make the request(s)
        status_changes = self._request(
            providers, mds.STATUS_CHANGES, params, paging, rate_limit)

        return status_changes
