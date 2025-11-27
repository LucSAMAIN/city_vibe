from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "city_vibe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def compute_checksum(path):
    import hashlib
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

def _extract_revenue_to_mongo():
    from pymongo import MongoClient
    import pandas as pd
    import os
    import redis

    # Connect to Redis
    redis_client = redis.Redis(
        host="redis-instance",
        port=6379,
        db=0
    )

    client = MongoClient(
        "mongodb://mongo:27017/",  
        username='admin',
        password='admin'
    )

    for year in range(2019, 2024):
        usecols = (range(0, 9), range(9, 13)) if year > 2021 else (range(1, 10), range(10, 14))
        if year < 2020:
            skip = 3
        elif year < 2022:
            skip = 6
        else:
            skip = 5

        if year == 2019 or year == 2022:
            skipfooter = 2
        elif year == 2020:
            skipfooter = 4
        elif year == 2021 and year == 2023:
            skipfooter = 0
        else:
            skipfooter = 6

        file_path = f"/opt/airflow/data/revenue/revenue_{year}.xlsx"
        checksum = compute_checksum(file_path)
        key = f"file_status:{file_path}"

        previous = redis_client.get(key)
        previous = previous.decode() if previous else None

        if previous == checksum:
            print(f"File unchanged: {file_path} → skipping...")
            continue

        # File is new or changed, update Redis
        redis_client.set(key, checksum)
        print(f"File changed: {file_path} → processing...")

        revenue_first_part = pd.read_excel(file_path, header=0, usecols=usecols[0], skiprows=skip, skipfooter=skipfooter)[1:].reset_index(drop=True)
        revenue_second_part = pd.read_excel(file_path, header=1, usecols=usecols[1], skiprows=skip, skipfooter=skipfooter)
        revenue = pd.concat([revenue_first_part, revenue_second_part], axis=1)
        
        collection_name = f"revenue_{year}"
        client["extracted"][collection_name].delete_many({})
        client["extracted"][collection_name].insert_many(
            revenue.to_dict(orient="records"),
        )
        logger.info(f"Inserted data from revenue_{year}.xlsx into MongoDB collection {collection_name}")

with DAG(
    dag_id="staging_dag",
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

    extracting_revenue = PythonOperator(
        task_id="extracting_revenue",
        python_callable=_extract_revenue_to_mongo,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )


    start >> extracting_revenue >> end
