import base64
import time
import logging

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
        
        AuthInfoStr = f"{str(self.connection.login)}/token:{str(self.connection.password)}"
        AuthInfoB = base64.b64encode(AuthInfoStr.encode("utf-8"))
        AuthInfoStr = str(AuthInfoB, "utf-8")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": f"application/json",
            "Authorization": f"Basic {AuthInfoStr}"
        })


    def _request(self, url, payload_key, params=None, results=[]):
        """
        Internal helper for sending requests.

        Returns payload(s).
        """
        retries = 0
        res = None

        self.log.debug(f"Making request to: {url}")
        logging.info(f"---------------------------payload key: {payload_key} params {params} url {url}  session {self.session.headers}")

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
        logging.info(f"-----------------Received response from {url} and {params}")

        page = res.json()

        next_start_time = 'null'
       
        
        if page[payload_key] is not None:
            results.extend(page[payload_key])

        end_of_stream = page["end_of_stream"]

        if end_of_stream == False:
            next_page = page["next_page"]

            if "rate_limit" in self.connection.extra_dejson:
                time.sleep(int(self.connection.extra_dejson["rate_limit"]))
            
            #if len(results) < 20000:
            results, next_start_time = self._request(url=next_page, payload_key=payload_key, results=results)
            #else:
                #next_start_time = page["end_time"]
        else:
            next_start_time = page["end_time"]

        return results, next_start_time


    #Call Zendesk Api and get data
    def get_tickets(self, start_time=None):

        logging.info(f"Retrieving tickets for period {start_time} to {datetime.now()}")

        # gather all the params together
        params = dict(start_time=start_time)

        # make the request(s)
        tickets, next_start_time = self._request(self.connection.host.replace(":endpoint", "incremental/tickets.json"), "tickets",params)

        return pd.DataFrame.from_records(tickets), next_start_time