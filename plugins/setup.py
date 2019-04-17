from setuptools import setup

with open('README') as f:
    readme = f.read()

setup(
    name='plugins',
    version='0.1.0',
    description='Airflow plugins for City of Portland Airflow instances',
    long_description=readme,
    author='Mike McDonald',
    author_email='michael.mcdonald@portlandoregon.gov',
    entry_points={
        'airflow.plugins': [
            'mobility_plugin = transportation_plugins.mobility_plugin:MobilityPlugin'
        ]
    }
)
