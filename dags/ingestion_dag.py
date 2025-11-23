from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

import logging

logger = logging.getLogger(__name__)

REVENUE_URLS = [
    (2023, "https://www.data.gouv.fr/api/1/datasets/r/65c61c89-ab5d-42dc-ac5b-03194f9d2efc"),
    (2022, "https://www.data.gouv.fr/api/1/datasets/r/35c857d5-7479-4c5f-9b0a-8036e24fbbb6"),
    (2021, "https://www.data.gouv.fr/api/1/datasets/r/261bc54d-d856-4d11-b4f8-f2b01e3073c6"),
    (2020, "https://www.data.gouv.fr/api/1/datasets/r/23c55b31-74c2-4669-9815-84b79d7c5d8f"),
    (2019, "https://www.data.gouv.fr/api/1/datasets/r/cc0d8dc0-6b13-4a86-bcf7-32418bf7b787")
]

default_args = {
    "owner": "city_vibe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _download_revenue():
    logger.info("Downloading revenue datasets...")
    import os
    import requests

    for date, url in REVENUE_URLS:
        if os.path.exists(f"/opt/airflow/data/revenue/revenue_{date}.zip"):
            logger.info(f"File revenue_{date}.zip already exists, skipping download.")
            continue
        res = requests.get(url)
        if res.status_code == 200:
            os.makedirs("/opt/airflow/data/revenue", exist_ok=True)
            with open(f"/opt/airflow/data/revenue/revenue_{date}.zip", "wb") as f:
                f.write(res.content)
            logger.info(f"Downloaded revenue_{date}.zip")
        else:
            logger.error(f"Error code {res.status_code} for {date} : {res.text}")
            raise Exception(f"Failed to download revenue data for {date}")

def _extract_revenue():
    logger.info("Extracting revenue datasets...")
    import zipfile
    import glob

    zip_files = glob.glob("/opt/airflow/data/revenue/*.zip")
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for member in z.namelist():
                    if member.endswith(".xlsx"):
                        name = zip_file.split('/')[-1].replace(".zip", ".xlsx")
                        with open(f"/opt/airflow/data/revenue/{name}", 'wb') as f:
                            f.write(z.read(member))
            logger.info(f"Extracted {zip_file}")

with DAG(
    dag_id="ingestion_dag",
    description="Schéma global des pipelines City Vibe (ingestion -> dimensions -> faits -> qualité -> mart)",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    default_args=default_args,
    tags=["city_vibe", "demo"],
) as dag:

    start = EmptyOperator(
        task_id="start",
        dag=dag,
    )

    download_revenue = PythonOperator(
        task_id="download_data",
        python_callable=_download_revenue,
        dag=dag,
    )

    extract_revenue = PythonOperator(
        task_id="extract_revenue",
        python_callable=_extract_revenue,
        dag=dag,
    )

    cleanup_revenue = BashOperator(
        task_id="cleanup_revenue",
        bash_command="rm -rf /opt/airflow/data/revenue/*.zip",
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )


    start >> download_revenue >> extract_revenue >> cleanup_revenue >> end
