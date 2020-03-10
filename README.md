# airflow
[![CircleCI](https://circleci.com/gh/mike-mcdonald/airflow/tree/master.svg?style=svg&circle-token=917f4ac9fbdc1406ed376aacd175600873520bc8)](https://circleci.com/gh/mike-mcdonald/airflow-base/tree/master)

This repository contains the code for running [PBOT's](https://portland.gov/transportation) data ingestion pipeline for e-scooter and other data sets.

## Information

* Based on Python (3.7-slim) official Image [python:3.6-slim](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue

## Build

From the project directory run:

    docker build -t pbotapps.azurecr.io/airflow .

## Run
    docker-compose up

You'll be able to see the Airflow admin UI at http://localhost:8080

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


## Scale the number of workers

Easy scaling using docker-compose:

    docker-compose scale worker=5

This can be used to scale to a multi node setup using docker swarm.
