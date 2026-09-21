"""Define the daily Airflow DAG for refreshing station-reference metadata.

Loaded by Airflow from the ``dags`` folder and calls
``scripts.refresh_station_reference.refresh_station_reference``.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow')
from scripts.refresh_station_reference import refresh_station_reference

default_args = {"retries": 2, "retry_delay": 300}

with DAG(
    dag_id="station_reference_refresh",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["citibike", "reference"],
) as dag:
    refresh = PythonOperator(
        task_id="refresh_station_reference",
        python_callable=refresh_station_reference,
    )