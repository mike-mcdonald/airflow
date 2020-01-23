import base64
import time

from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

class ZendeskHook(BaseHook):
    def __init__(self,
                 max_retries=3,
                 zendesk_conn_id="Zendesk_API"):
        self.max_retries = max_retries

        try:
            self.connection = self.get_connection(zendesk_conn_id)
        except AirflowException as err:
            self.log.error(
                f"Failed to find connection for Zendesk, Error: {err}")
            raise
        
        AuthInfo = base64.b64encode(f"{self.connection.login}/token:{self.connection.password}")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": f"application/json",
            "Authorization": f"Basic {AuthInfo}"
        })

    def _date_format(self, dt):
        return int(dt.timestamp()) * 1000

    def _request(self, url, payload_key, params=None, results=[]):
        """
        Internal helper for sending requests.

        Returns payload(s).
        """
        retries = 0
        res = None

        self.log.debug(f"Making request to: {url}")

        while res is None:
            try:
                res = self.session.get(url, params=params)
                res.raise_for_status()
            except Exception as err:
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

        page = res.json()

        if page["tickets"] is not None:
            results.extend(page["tickets"][payload_key])

        if page["end_of_stream"] == False:
            next_page = page["next_page"]
            if "rate_limit" in self.connection.extra_dejson:
                time.sleep(int(self.connection.extra_dejson["rate_limit"])) 
            results = self._request(url=next_page, payload_key=payload_key,
                                        results=results)

        return results


    def get_tickets(self, start_time=None):
        """
        Request Status Changes data. Returns a DataFrame of status_changes payload(s)

        Supported keyword args:

            - `start_time`: Filters for status changes where `event_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for status changes where `event_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

        """

        self.log.debug(
            f"Retrieving events for period {start_time} to now")

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        

        # gather all the params together
        params = dict(start_time=start_time)

        # make the request(s)
        tickets = self._request(
            self.connection.host.replace(":endpoint", "incremental/tickets.json"), "incremental/tickets.json", params)

        return pd.DataFrame.from_records(tickets)