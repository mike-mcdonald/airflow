# airflow
[![CircleCI](https://circleci.com/gh/mike-mcdonald/airflow/tree/master.svg?style=svg&circle-token=917f4ac9fbdc1406ed376aacd175600873520bc8)](https://circleci.com/gh/mike-mcdonald/airflow-base/tree/master)

This repository contains the code for running [PBOT's](https://portland.gov/transportation) data ingestion pipeline for e-scooter and other data sets.

## Information

* Based on Python (3.7-slim) [official Image](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue

## Development workflow

### Build

From the project directory run:

    docker build -t pbotapps.azurecr.io/airflow .

### Run
    docker-compose up

### Reset
    docker-compose down
    docker-compose up

You'll be able to see the Airflow admin UI at http://localhost:8080

### UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


### Scale the number of workers

Easy scaling using docker-compose:

    docker-compose up --scale worker=5

## Access control
This project is configured to use Azure AD as an OAuth provider in the `config/webserver_config.py` file.  The configuration of the IDs and secrets that are required to follow the OAuth flow are provided by environment variables.  Locally, this can be done with a `.env` file at the top level of the project.  You can copy `.env.example` as `.env` and set appropriate values from your [Azure App registration](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app) to get started.

## Code changes
Code changes to DAG files will update automatically during development because they are mapped as a volume in the compose file.  Code changes to plugins require a re-build of the container, which is documented in the Build section above.  This is because plugins are installed as a python package.  A possible alternative to re-building, which should be a rapid process, would be to run bash in each of the webserver, scheduler, worker and flower containers and re-run the following command:

    pip install --user -e plugins

Files in the config folder may be mapped as volumes in the containers in the compose file with limited success, so it is better to re-build the container and restart when changing configuration.

## Releases and deployment
This project uses CI from CircleCI to manage releases to the cluster(s) running this project.  Locally, [git flow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) works well to manage the branches and releases and the following release procedure is based on git flow.  When code is ready for deployment, determine the next version in the form X.X.X.X where X is a number by noting the Airflow version specified in the Dockerfile and searching through the current tags.  This project uese the Airflow version as the first three parts of the version and the last portion is incremented on each release.  The following changes then need to be made before updating master and pushing a new tag:
* In the `k8s` folder, update all the versions in the following files spec/template/spec/containers/image configuration variable:
  * `deployments-flower.yaml`
  * `deployments-scheduler.yaml`
  * `deployments-web.yaml`
  * `statefulsets-workers.yaml`

Once that is completed, you are ready to kick off the deployment workflow by pushing the master branch, then tagging your work and pushing that.  The CircleCI badge at the top of this README will take you to the correct palce to monitor the deployment jobs

