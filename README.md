# airflow-base

This repository contains **Dockerfile** of [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/)

## Information

* Based on Python (3.7-slim) official Image [python:3.6-slim](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue

## Build

From the project directory run:

    docker build -t pbotapps.azurecr.io/airflow/base .

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
