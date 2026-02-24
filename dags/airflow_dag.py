"""
Airflow DAG: Northwind medallion pipeline (Bronze -> Silver -> Gold).
Single task runs the full pipeline. Schedule: daily.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# In docker-compose we mount project under /opt/airflow (dags, src, data, ...)
PROJECT_ROOT = "/opt/airflow"

# Pipeline deps are installed as airflow user into ~/.local; task subprocess needs this on PYTHONPATH
AIRFLOW_SITE_PACKAGES = "/home/airflow/.local/lib/python3.11/site-packages"

with DAG(
    dag_id="northwind_sales_pipeline",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    description="Northwind sales: Bronze -> Silver -> Gold -> output",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["northwind", "medallion"],
) as dag:
    BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd '{PROJECT_ROOT}' && PYTHONPATH='{PROJECT_ROOT}:{AIRFLOW_SITE_PACKAGES}' python -m src.main run",
    )
