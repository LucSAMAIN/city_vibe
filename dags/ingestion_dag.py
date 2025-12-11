from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

import logging

## Configuration

logger = logging.getLogger(__name__)
default_args = {
    "owner": "city_vibe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

## Constant definitions

REVENUE_URLS = [
    (2023, "https://www.data.gouv.fr/api/1/datasets/r/65c61c89-ab5d-42dc-ac5b-03194f9d2efc"),
    (2022, "https://www.data.gouv.fr/api/1/datasets/r/35c857d5-7479-4c5f-9b0a-8036e24fbbb6"),
    (2021, "https://www.data.gouv.fr/api/1/datasets/r/261bc54d-d856-4d11-b4f8-f2b01e3073c6"),
    (2020, "https://www.data.gouv.fr/api/1/datasets/r/23c55b31-74c2-4669-9815-84b79d7c5d8f"),
    (2019, "https://www.data.gouv.fr/api/1/datasets/r/cc0d8dc0-6b13-4a86-bcf7-32418bf7b787")
]

DVF_URL = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-geolocalisees/20251105-140205/dvf.csv.gz"

## Helper functions

def compute_checksum(path):
    import hashlib  
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

## Task functions
# REVENUES
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

# DVF

def _download_dvf():
    logger.info("Downloading DVF dataset...")
    import os
    import requests

    if os.path.exists("/opt/airflow/data/dvf/dvf.csv.gz"):
        logger.info("File dvf.csv.gz already exists, skipping download.")
        return

    res = requests.get(DVF_URL)
    if res.status_code == 200:
        os.makedirs("/opt/airflow/data/dvf", exist_ok=True)
        with open("/opt/airflow/data/dvf/dvf.csv.gz", "wb") as f:
            f.write(res.content)
        logger.info("Downloaded dvf.csv.gz")
    else:
        logger.error(f"Error code {res.status_code} : {res.text}")
        raise Exception("Failed to download DVF data")

def _unzip_dvf():
    logger.info("Unzipping DVF dataset...")
    import gzip
    import shutil
    import os

    input_path = "/opt/airflow/data/dvf/dvf.csv.gz"
    output_path = "/opt/airflow/data/dvf/dvf.csv"

    if not os.path.exists(input_path):
        raise Exception("DVF gzip file does not exist.")

    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logger.info("Unzipped dvf.csv.gz to dvf.csv")

def _cleanup_dvf():
    import os
    os.remove("/opt/airflow/data/dvf/dvf.csv.gz")
    logger.info("Cleaned up dvf.csv.gz")


def _dvf_hash_redis():
    """
    Check if the DVF file has changed using Redis to store the checksum.
    Returns True if the file is new or changed, False if unchanged.
    """
    import redis
    import os

    # Connect to Redis
    redis_client = redis.Redis(
        host="redis-instance",
        port=6379,
        db=0
    )

    file_path = "/opt/airflow/data/dvf/dvf.csv"
    checksum = compute_checksum(file_path)
    key = f"file_status:{file_path}"

    previous = redis_client.get(key)
    previous = previous.decode() if previous else None

    if previous == checksum:
        print(f"File unchanged: {file_path} → skipping...")
        return "end"  # Branch to end task

    # File is new or changed, update Redis
    redis_client.set(key, checksum)
    print(f"File changed: {file_path} → processing...")

    return "dvf_to_mongo"  # Branch to dvf_to_mongo task

def _dvf_to_mongo():
    from pymongo import MongoClient
    import pandas as pd
    from dotenv import load_dotenv
    import os
    import time
    load_dotenv()

    # Define connection details
    client_args = {
        "host": "mongodb://mongo:27017/",
        "username": "admin",
        "password": "admin",
        "serverSelectionTimeoutMS": 60000,
        "connectTimeoutMS": 60000,
        "socketTimeoutMS": 60000
    }

    # Clear the collection once before starting the loop
    client = MongoClient(**client_args)
    db = client["extracted"]
    collection = db["dvf"]
    collection.delete_many({})
    logger.info("Cleared 'dvf' collection in MongoDB.")

    # Read the DVF CSV file and insert into MongoDB
    logger.info("Inserting DVF data into MongoDB...")
    file_path = "/opt/airflow/data/dvf/dvf.csv"
    # df = pd.read_csv(file_path, engine='c', low_memory=False)
    # Cant do this because of memory issues, so we do it in chunks
    chunk_size = 25000
    df_iterator = pd.read_csv(file_path, low_memory=False, chunksize=chunk_size)
    for i, df in enumerate(df_iterator):
        records = df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
            logger.info(f"Inserted chunk {i+1} with {len(records)} records into MongoDB 'dvf' collection.")
        time.sleep(1)  # Small delay to avoid overwhelming the database
    

    
    logger.info("Finished inserting DVF data into MongoDB.")
    

with DAG(
    dag_id="ingestion_dag",
    description="Ingestion data pipeline. Downloads data from data sources and inserts it into MongoDB.",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    default_args=default_args,
    tags=["city_vibe", "demo"],
) as dag:

    # Common start and end tasks
    start = EmptyOperator(
        task_id="start",
        dag=dag,
    )
    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )

    # REVENUE tasks
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

    revenue_to_mongo = PythonOperator(
        task_id="revenue_to_mongo",
        python_callable=_extract_revenue_to_mongo,
        dag=dag,
    )

    # DVF tasks
    download_dvf = PythonOperator(
        task_id="download_dvf",
        python_callable=_download_dvf,
        dag=dag,
    )

    unzip_dvf = PythonOperator(
        task_id="unzip_dvf",
        python_callable=_unzip_dvf,
        dag=dag,
    )

    cleanup_dvf = PythonOperator(
        task_id="cleanup_dvf",
        python_callable=_cleanup_dvf,
        dag=dag,
    )

    dvf_hash_redis = BranchPythonOperator(
        task_id="dvf_hash_redis",
        python_callable=_dvf_hash_redis,
        dag=dag,
    )

    dvf_to_mongo = PythonOperator(
        task_id="dvf_to_mongo",
        python_callable=_dvf_to_mongo,
        dag=dag,
    )


    # Define task dependencies

    start >> [download_dvf, download_revenue]

    # DVF workflow
    download_dvf >> unzip_dvf >> cleanup_dvf >> dvf_hash_redis
    dvf_hash_redis >> [dvf_to_mongo, end]

    # REVENUE workflow
    download_revenue >> extract_revenue >> cleanup_revenue >> revenue_to_mongo

    [dvf_to_mongo, revenue_to_mongo] >> end
