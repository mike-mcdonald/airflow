FROM airflow
COPY script/entrypoint.sh /entrypoint.sh
COPY plugins ${AIRFLOW_HOME}/plugins
COPY dags ${AIRFLOW_HOME}/dags
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
