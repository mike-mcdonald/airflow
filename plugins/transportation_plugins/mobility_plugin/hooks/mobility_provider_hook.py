from oauthlib.oauth2 import BackendApplicationClient
from requests import Session
from requests_oauthlib import OAuth2Session

from airflow.hooks.base_hook import BaseHook


class MobilityProviderHook(BaseHook):
    def __init__(self,
                 version="0.3.0",
                 mobility_provider_conn_id='mobility_provider_default',
                 mobility_provider_token_conn_id=None):
        try:
            self.connection = self.get_connection(mobility_provider_conn_id)
            self.token_connection = self.get_connection(
                mobility_provider_token_conn_id)
        except:
            self.log.error(f"Failed to find connections for mobility provider")

        if mobility_provider_token_conn_id is not None:
            client = BackendApplicationClient(
                client_id=self.token_connection.login)
            oauth = OAuth2Session(client=client)
            token = oauth.fetch_token(token_url=self.token_connection.host, client_id=self.token_connection.login,
                                      client_secret=self.token_connection.password)
            self.session = OAuth2Session(client_id=self.token_connection.login,
                                         scope=self.token_connection.extra_dejson["scope"])
        else:
            self.session = Session()

    def __request(self, url, params, results=[]):
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
            results.append(page["data"])

        if "links" in page:
            next_page = page["links"].get("next")
            if next_page is not None:
                self.__request(url=next_page, params=None, results=results)

        return results

    def get_trips(self, device_id=None, vehicle_id=None, min_end_time=None, max_end_time=None):
        """
        Request Trips data. Returns a dict of provider => list of trips payload(s).

        Supported keyword args:

            - `device_id`: Filters for trips taken by the given device.

            - `vehicle_id`: Filters for trips taken by the given vehicle.

            - `start_time`: Filters for trips where `start_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for trips where `end_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds
        """
        if providers is None:
            providers = self.providers

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
        trips = self.__request("trips", params)

        return trips

    def get_events(self, start_time=None, end_time=None):
        """
        Request Status Changes data. Returns a dict of provider => list of status_changes payload(s)

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
        status_changes = self._request("status_changes", params)

        return status_changes
