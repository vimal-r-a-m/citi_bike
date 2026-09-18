from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from datetime import datetime
import sys, duckdb

sys.path.append('/opt/airflow')
from scripts.download import download_and_extract
from scripts.upload_to_minio import upload_extracted_csvs
from scripts.transform import clean_trips, write_silver

default_args = {"retries": 2, "retry_delay": 300}


def _download_task(**context):
    year_month = context["params"]["year_month"]
    extracted = download_and_extract(year_month)
    return extracted  # goes to XCom


def _upload_task(**context):
    extracted = context["ti"].xcom_pull(task_ids="download_and_extract")
    year_month = context["params"]["year_month"]
    upload_extracted_csvs(extracted, year_month)


def _clean_task(**context):
    year_month = context["params"]["year_month"]
    year, month = year_month[:4], year_month[4:6]
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Add these two lines to prevent out-of-memory kills
    con.execute("PRAGMA memory_limit='4GB';")
    con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
    con.execute("""
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)
    clean_trips(con, f"s3://bronze/citibike_extracted/{year}/{month}/*.csv", source_month=year_month)
    write_silver(con)


with DAG(
    dag_id="citibike_batch_pipeline",
    start_date=datetime(2026, 6, 1),
    schedule_interval="@monthly",
    catchup=False,
    default_args=default_args,
    params={"year_month": Param("202606", type="string")},
    tags=["citibike", "batch"],
) as dag:

    download_and_extract_task = PythonOperator(
        task_id="download_and_extract",
        python_callable=_download_task,
    )

    upload_bronze = PythonOperator(
        task_id="upload_to_bronze",
        python_callable=_upload_task,
    )

    clean_to_silver = PythonOperator(
        task_id="clean_to_silver",
        python_callable=_clean_task,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_gold",
        bash_command="cd /opt/airflow/citibike_dbt && dbt run --profiles-dir . && dbt test --profiles-dir .",
    )

    download_and_extract_task >> upload_bronze >> clean_to_silver >> run_dbt