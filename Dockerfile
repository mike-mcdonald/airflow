FROM python:3.7-slim

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.2
ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_GPL_UNIDECODE yes

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Use CoP certificates
COPY ./.certs /usr/local/share/ca-certificates/
RUN update-ca-certificates
ENV REQUESTS_CA_BUNDLE /etc/ssl/certs/ca-certificates.crt

COPY script/entrypoint.sh /entrypoint.sh

# Install general requirements
RUN set -ex \
    && buildDeps='\
    apt-transport-https \
    dos2unix \
    freetds-dev \
    gnupg2 \
    libkrb5-dev \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    ' \
    && runDeps='\
    apt-utils \
    build-essential \
    curl \
    default-libmysqlclient-dev \
    freetds-bin \
    gdal-bin \
    git \
    locales \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libspatialindex-dev \
    netcat \
    proj-bin \
    proj-data \
    python-gdal \
    rsync \
    unixodbc \
    unixodbc-dev \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
    $buildDeps \
    $runDeps \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    # Install Microsoft ODBC driver
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    # Clean up any bad line endings
    && dos2unix /entrypoint.sh \
    # Install Airflow python dependencies
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[all]==${AIRFLOW_VERSION} \
    && pip install redis \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

# Install specific airflow dependencies
COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Install custom plugins as package
COPY plugins /usr/local/plugins
RUN pip install -e /usr/local/plugins

COPY ./dags /usr/local/airflow/dags
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
