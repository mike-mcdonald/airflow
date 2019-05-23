from setuptools import setup

setup(
    name='plugins',
    version='0.1.0',
    description='Airflow plugins for City of Portland Airflow instances',
    author='Mike McDonald',
    author_email='michael.mcdonald@portlandoregon.gov',
    entry_points={
        'airflow.plugins': [
            'mobility_plugin = transportation_plugins.mobility_plugin:MobilityPlugin',
            'dataframe_plugin = common_plugins.dataframe_plugin:DataFramePlugin',
            'calendar_plugin = common_plugins.calendar_plugin:CalendarPlugin',
            'mssql_plugin = common_plugins.mssql_plugin:MsSqlPlugin'
        ]
    }
)
