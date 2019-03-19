import gzip
import bz2
import tempfile
import os
import json
from builtins import next
from builtins import zip
from oauthlib.oauth2 import BackendApplicationClient
from requests import Session
from requests_oauthlib import OAuth2Session
from tempfile import NamedTemporaryFile

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory

from airflow.contrib.hooks import AzureDataLakeHook
# Will show up under airflow.hook.mobility_plugin.MobilityProviderHook


class MobilityProviderHook(BaseHook):
    def __init__(self,
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

    def get_trips(self, device_id, vehicle_id, min_end_time, max_end_time):
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
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params togethers
        params = {
            **dict(device_id=device_id, vehicle_id=vehicle_id, start_time=start_time, end_time=end_time),
        }

        # make the request(s)
        trips = self._request(providers, "trips", params)

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
        status_changes = self._request(
            providers, "status_changes", params)

        return status_changes

# Will show up under airflow.operators.mobility_plugin.MobilityProviderToAzureDataLakeOperator


class MobilityTripsToAzureDataLakeOperator(BaseOperator):
    """

    """

    def __init__(self,
                 mobility_provider_conn_id="mobility_provider_default",
                 mobility_provider_token_conn_id=None,
                 azure_data_lake_conn_id="azure_data_lake_default",
                 remote_path="",
                 min_end_time=None,
                 max_end_time=None,
                 * args, **kwargs):
        super(MobilityTripsToAzureDataLakeOperator,
              self).__init__(*args, **kwargs)
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.mobility_provider_conn_id = mobility_provider_conn_id,
        self.mobility_provider_token_conn_id = mobility_provider_token_conn_id
        self.remote_path = remote_path
        self.min_end_time = min_end_time
        self.max_end_time = max_end_time

    def execute(self, context):
        # Create the hook
        hook = MobilityProviderHook(
            mobility_provider_conn_id=self.mobility_provider_conn_id,
            mobility_provider_token_conn_id=self.mobility_provider_token_conn_id)

        # Get trips
        trips = hook.get_trips(
            min_end_time=self.min_end_time, max_end_time=self.max_end_time)

        # Write to temp file
        with TemporaryDirectory(prefix='tmps32mds_') as tmp_dir,\
                NamedTemporaryFile(mode="wb",
                                   dir=tmp_dir,
                                   suffix=json) as f:
            self.log.info("Dumping trips to local file {1}"
                          .format(f.name))
            f.write(json.dumps(trips))
            f.flush()

            # Upload to Azure Data Lake
            hook = AzureDataLakeHook(
                azure_data_lake_conn_id=self.azure_data_lake_conn_id)

            hook.upload_file(
                local_path=f.name,
                remote_path=self.remote_path)

            # Delete file
            f.delete()

        return


# Defining the plugin class


class MobilityPlugin(AirflowPlugin):
    name = "mobility_plugin"
    operators = [MobilityTripsToAzureDataLakeOperator]
    hooks = [MobilityProviderHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
