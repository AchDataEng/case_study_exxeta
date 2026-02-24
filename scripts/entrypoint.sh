#!/usr/bin/env bash
"""
Create pipeline dirs on named volumes and chown to airflow so the pipeline can write
"""
set -e
mkdir -p /opt/airflow/datalake/bronze/orders \
         /opt/airflow/datalake/bronze/products \
         /opt/airflow/datalake/silver/order_lines \
         /opt/airflow/datalake/gold \
         /opt/airflow/logs \
         /opt/airflow/output
chown -R airflow:root /opt/airflow/datalake /opt/airflow/logs /opt/airflow/output
exec /entrypoint "$@"
