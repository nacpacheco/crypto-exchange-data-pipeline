FROM apache/airflow:2.8.1
USER root
COPY dags/ /opt/airflow/dags/
USER airflow