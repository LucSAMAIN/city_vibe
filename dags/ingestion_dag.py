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
OFFLINE_MODE = True

## Constant definitions

REVENUE_IDS = [
    (2023, "65c61c89-ab5d-42dc-ac5b-03194f9d2efc"),
    (2022, "35c857d5-7479-4c5f-9b0a-8036e24fbbb6"),
    (2021, "261bc54d-d856-4d11-b4f8-f2b01e3073c6"),
    (2020, "23c55b31-74c2-4669-9815-84b79d7c5d8f"),
    (2019, "cc0d8dc0-6b13-4a86-bcf7-32418bf7b787")
]

DVF_URL = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-geolocalisees/20251105-140205/dvf.csv.gz"

OUTPUT_DPE_PATH = "/opt/airflow/data/offline-data/dpe" if OFFLINE_MODE else "/opt/airflow/data/dpe"
OUTPUT_DPE_FILE = "dpe_subset.ndjson" if OFFLINE_MODE else "dpe_03_raw.ndjson"

## Helper functions

def compute_checksum(path):
    import hashlib  
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

def get_checksum(url):
    import requests

    response = requests.get(url)
    response.raise_for_status()
    
    metadata = response.json()
    current_checksum = metadata.get('checksum', {}).get('value')
    
    if not current_checksum:
        current_checksum = metadata.get('last_modified')
        logger.error("Warning: No checksum found, using last_modified.")

    return current_checksum

## Task functions
# REVENUES

def _revenue_hash_redis(**context):
    """
    Check if the revenue file has changed using Redis to store the checksum.
    Returns True if the file is new or changed, False if unchanged.
    """
    import redis
    import os

    if OFFLINE_MODE:
        logger.info("OFFLINE_MODE detected. Skipping download/hash check. Using local subsets.")
        # On saute le téléchargement et l'extraction pour aller directement à l'insertion
        return "revenue_to_mongo"

    # Connect to Redis
    redis_client = redis.Redis(
        host="redis-instance",
        port=6379,
        db=0
    )

    DATASET_ID = "536998cba3a729239d20505e"
    next_task = "end"
    to_download = []
    for date, id in REVENUE_IDS:
        file_path = "/opt/airflow/data/revenue/revenue_" + str(date) + ".xlsx"
        url = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/resources/{id}/"

        checksum = get_checksum(url)
        key = f"file_status:{file_path}"

        previous = redis_client.get(key)
        previous = previous.decode() if previous else None

        if previous == checksum:
            logger.info(f"File unchanged: {file_path} → skipping...")
            continue

        # File is new or changed, update Redis
        to_download.append(id)
        next_task = "download_revenue"
        redis_client.set(key, checksum)
        logger.info(f"File changed: {file_path} → processing...")

    context["ti"].xcom_push(key="to_download", value=to_download)

    return next_task  # Branch to next task

def _download_revenue(**context):
    logger.info("Downloading revenue datasets...")
    import os
    import requests

    to_download = context["ti"].xcom_pull(key="to_download", task_ids="revenue_hash_redis")
    for date, id in REVENUE_IDS:
        if id not in to_download:
            logger.info(f"File revenue_{date}.zip is up to date, skipping download.")
            continue
        res = requests.get("https://www.data.gouv.fr/api/1/datasets/r/" + id)
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

        file_path = f"/opt/airflow/data/offline-data/revenue/revenue_{year}.xlsx" if OFFLINE_MODE else f"/opt/airflow/data/revenue/revenue_{year}.xlsx"

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

    os.makedirs("/opt/airflow/data/dvf", exist_ok=True)
    output_path = "/opt/airflow/data/dvf/dvf.csv.gz"

    with requests.get(DVF_URL, stream=True) as res:
        if res.status_code == 200:
            with open(output_path, "wb") as f:
                for chunk in res.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            
            logger.info("Downloaded dvf.csv.gz successfully")
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

    if OFFLINE_MODE:
        return

    if os.path.exists("/opt/airflow/data/dvf/dvf.csv.gz"):
        os.remove("/opt/airflow/data/dvf/dvf.csv.gz")
        logger.info("Cleaned up dvf.csv.gz")
    else:
        logger.warning("dvf.csv.gz does not exist, nothing to clean up.")
    

def _dvf_hash_redis():
    """
    Check if the DVF file has changed using Redis to store the checksum.
    Returns True if the file is new or changed, False if unchanged.
    """
    import redis
    import os

    if OFFLINE_MODE:
        logger.info("OFFLINE_MODE detected. Skipping download. Using local subset.")
        return "dvf_to_mongo"

    # Connect to Redis
    redis_client = redis.Redis(
        host="redis-instance",
        port=6379,
        db=0
    )

    file_path = "/opt/airflow/data/dvf/dvf.csv"
    DATASET_ID = "5cc1b94a634f4165e96436c1"
    RESOURCE_ID = "d7933994-2c66-4131-a4da-cf7cd18040a4"
    # https://www.data.gouv.fr/api/1/datasets/5cc1b94a634f4165e96436c1/resources/d7933994-2c66-4131-a4da-cf7cd18040a4/
    url = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/resources/{RESOURCE_ID}/"
    checksum = get_checksum(url)
    key = f"file_status:{file_path}"

    previous = redis_client.get(key)
    previous = previous.decode() if previous else None

    if previous == checksum:
        logger.info(f"File unchanged: {file_path} → skipping...")
        return "end"  # Branch to end task

    # File is new or changed, update Redis
    redis_client.set(key, checksum)
    logger.info(f"File changed: {file_path} → processing...")

    return "download_dvf"  # Branch to download_dvf task

# AURA departments for DVF filtering (2-digit codes)
ARA_DEPARTMENTS_DVF = {
    "01",  # Ain
    "03",  # Allier
    "07",  # Ardèche
    "15",  # Cantal
    "26",  # Drôme
    "38",  # Isère
    "42",  # Loire
    "43",  # Haute-Loire
    "63",  # Puy-de-Dôme
    "69",  # Rhône
    "73",  # Savoie
    "74",  # Haute-Savoie
}

def _dvf_to_mongo():
    from pymongo import MongoClient
    import pandas as pd
    # from dotenv import load_dotenv
    import os
    import time
    # load_dotenv()

    def normalize_department_code(val):
        """Normalize department code to 2-char string (e.g., '1' -> '01')"""
        if pd.isna(val) or val is None:
            return None
        s = str(val).strip()
        if s.isdigit():
            return s.zfill(2)
        return s.upper()

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
    logger.info("Inserting DVF data into MongoDB (AURA only)...")
    file_path = "/opt/airflow/data/offline-data/dvf/dvf_subset.csv" if OFFLINE_MODE else "/opt/airflow/data/dvf/dvf.csv"
    # Cant do this because of memory issues, so we do it in chunks
    chunk_size = 50000
    df_iterator = pd.read_csv(file_path, 
                              low_memory=False, 
                              chunksize=chunk_size,
                              engine='c')
    
    total_inserted = 0
    total_filtered = 0
    
    for i, df in enumerate(df_iterator):
        initial_count = len(df)
        
        # Filter for AURA departments only
        df['code_departement'] = df['code_departement'].apply(normalize_department_code)
        df = df[df['code_departement'].isin(ARA_DEPARTMENTS_DVF)]
        
        filtered_count = initial_count - len(df)
        total_filtered += filtered_count
        
        records = df.to_dict(orient="records")
        if records:
            # Use unordered=True for faster bulk inserts (no guarantee of insertion order)
            collection.insert_many(records, ordered=False)
            total_inserted += len(records)
            logger.info(f"Chunk {i+1}: inserted {len(records)} AURA records (filtered out {filtered_count} non-AURA)")
        time.sleep(0.1)  # Small delay to avoid overwhelming the database
    
    logger.info(f"Finished inserting DVF data into MongoDB. Total: {total_inserted} AURA records (filtered out {total_filtered} non-AURA)")
    

## DPE

## Task functions

def _download_dpe():
    logger.info("Downloading DPE dataset...")
    import os
    import requests
    import json
    import time

    if OFFLINE_MODE:
        logger.info("OFFLINE_MODE: Skipping DPE download. Assuming 'dpe_subset.ndjson' exists.")
        return

    #fiel dpe : numero_dpe, date_etablissement_dpe, etiquette_dpe, etiquette_ges, numero_voie_ban, nom_rue_ban, nom_commune_ban, code_postal_ban, code_insee_ban
    # ,identifiant_ban

    # --- CONFIGURATION ---
    os.makedirs(f"{OUTPUT_DPE_PATH}", exist_ok=True)

    DPE_URL = (
        "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines?"
        "format=json&size=10000&"
        "code_departement_ban_in=01,03,07,15,26,38,42,43,63,69,73,74&"
        "select=numero_dpe,date_etablissement_dpe,etiquette_dpe,etiquette_ges,"
        "numero_voie_ban,nom_rue_ban,nom_commune_ban,code_postal_ban,"
        "code_insee_ban,identifiant_ban,"
        "conso_5_usages_par_m2_ep,emission_ges_5_usages_par_m2,"
        "type_batiment"
    )

    session = requests.Session()
    url = DPE_URL
    total_lines = 0
    start_time = time.time()


    logger.info(f"Début du téléchargement vers {OUTPUT_DPE_PATH}...")


    # On ouvre le fichier en mode écriture ('w') avec encodage UTF-8
    with open(f"{OUTPUT_DPE_PATH}/{OUTPUT_DPE_FILE}", 'w', encoding='utf-8') as f:
        while url:
            try:
                # 1. Requête API
                response = session.get(url)
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                if not results:
                    break
                
                # 2. Écriture NDJSON
                for row in results:
                    json_line = json.dumps(row, ensure_ascii=False)
                    f.write(json_line + '\n')
                
                total_lines += len(results)
                

                logger.info(f"{total_lines} lignes sauvegardées...")

                # 3. Pagination
                url = data.get('next')

            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur réseau : {e}")
                raise e

    duration = (time.time() - start_time) / 60
    logger.info(f"Téléchargement terminé. {total_lines} lignes en {duration:.2f} min.")


def _dpe_hash_redis():
    """
    Check if the DPE file has changed using Redis to store the checksum.
    Returns True if the file is new or changed, False if unchanged.
    """
    import redis
    import os

    if OFFLINE_MODE:
        return "dpe_to_mongo"

    # Connect to Redis
    redis_client = redis.Redis(
        host="redis-instance",
        port=6379,
        db=0
    )
    file_path=f"{OUTPUT_DPE_PATH}/{OUTPUT_DPE_FILE}"
    checksum = compute_checksum(file_path)
    key = f"file_status:{file_path}"

    previous = redis_client.get(key)
    previous = previous.decode() if previous else None

    if previous == checksum:
        logger.info(f"File unchanged: {file_path} → skipping...")
        return "end"  # Branch to end task

    # File is new or changed, update Redis
    redis_client.set(key, checksum)
    logger.info(f"File changed: {file_path} → processing...")

    return "dpe_to_mongo"  # Branch to dpe_to_mongo task



def python_bulk_import(**context):
    import json
    from pymongo import MongoClient

    # Connexion Mongo
    client = MongoClient(
        "mongodb://mongo:27017/",  
        username='admin',
        password='admin'
    )
    db = client["extracted"]
    collection = db["dpe"]

    logger.info("Drop de la collection existante...")
    collection.drop()
    
    # 4. Lecture et Insertion par Batch (Optimisé pour la RAM et la Vitesse)
    BATCH_SIZE = 100000 
    buffer = []
    total_count = 0
    file_path = f"{OUTPUT_DPE_PATH}/{OUTPUT_DPE_FILE}"
    logger.info(f"Démarrage de l'import depuis {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue # Saute les lignes vides
            
            # Parsing rapide du JSON
            doc = json.loads(line)
            buffer.append(doc)

            if len(buffer) >= BATCH_SIZE:
                collection.insert_many(buffer, ordered=False)
                total_count += len(buffer)
                logger.info(f"-> {total_count} documents insérés...")
                buffer = [] # Reset du buffer

        # Insertion des derniers éléments restants
        if buffer:
            collection.insert_many(buffer, ordered=False)
            total_count += len(buffer)

    logger.info(f"Terminé ! Total : {total_count} documents.")



with DAG(
    dag_id="ingestion-Revenu",
    description="Ingestion data pipeline for revenu data. Downloads data from data sources and inserts it into MongoDB.",
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
        trigger_rule="none_failed",
    )

    # REVENUE tasks

    revenue_hash_redis = BranchPythonOperator(
        task_id="revenue_hash_redis",
        python_callable=_revenue_hash_redis,
        dag=dag,
    )

    download_revenue = PythonOperator(
        task_id="download_revenue",
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
        trigger_rule="none_failed_min_one_success",
        dag=dag,
    )


    # Define task dependencies

    # REVENUE workflow
    start >> revenue_hash_redis >> [download_revenue, revenue_to_mongo, end] 
    download_revenue >> extract_revenue >> revenue_to_mongo >> cleanup_revenue >> end


with DAG(
    dag_id="ingestion-Dvf",
    description="Ingestion data pipeline for dvf data. Downloads data from data sources and inserts it into MongoDB.",
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
        trigger_rule="none_failed",
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
        trigger_rule="none_failed_min_one_success",
        dag=dag,
    )

    # Define task dependencies

    # DVF workflow
    start >> dvf_hash_redis >> [download_dvf, dvf_to_mongo, end]
    download_dvf >> unzip_dvf >> dvf_to_mongo >> cleanup_dvf >> end

with DAG(
    dag_id="ingestion-Dpe",
    description="Ingestion data pipeline for dpe data. Downloads data from data sources and inserts it into MongoDB.",
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
        trigger_rule="none_failed",
    )
   
    # DPE tasks 

    download_dpe = PythonOperator(
        task_id="download_dpe",
        python_callable=_download_dpe,
        dag=dag,
    )

    dpe_hash_redis = BranchPythonOperator(
        task_id="dpe_hash_redis",
        python_callable=_dpe_hash_redis,
        dag=dag,
    )

    dpe_to_mongo= PythonOperator(
        task_id='dpe_to_mongo',
        python_callable=python_bulk_import
    )
    # Define task dependencies

    #DPE workflow
    start >> download_dpe >> dpe_hash_redis >> [dpe_to_mongo, end]
    dpe_to_mongo >> end
