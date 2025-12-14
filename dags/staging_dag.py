from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

import logging

## Configuration

logger = logging.getLogger(__name__)

default_args = {
    "owner": "city_vibe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

## Constant definitions
COLUMN_TYPE_MAPPING = {
    "dep": "TEXT",
    "commune": "TEXT",
    "libelle_de_la_commune": "TEXT",
    "nombre_de_foyers_fiscaux": "INTEGER",
    "revenu_fiscal_de_reference_des_foyers_fiscaux": "FLOAT",
    "impot_net_total": "FLOAT",
    "nombre_de_foyers_fiscaux_imposes": "INTEGER",
    "revenu_fiscal_de_reference_des_foyers_fiscaux_imposes": "FLOAT",
}

## Helper functions for data cleaning and transfer
def clean_column(name):
    import re
    import unicodedata

    # Remove accents
    name = ''.join(
        c for c in unicodedata.normalize('NFD', name)
        if unicodedata.category(c) != 'Mn'
    )
    # Replace non-alphanumeric by underscore
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    # Lowercase
    return name.lower()

## Task functions
def revenue_mongo_to_postgres():
    import psycopg2
    from pymongo import MongoClient
    import pandas as pd
    import numpy as np
    import io

    # Connect to Mongo
    client = MongoClient(
        "mongodb://mongo:27017/",
        username="admin",
        password="admin"
    )

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS REVENUE;')

    collections = client.extracted.list_collection_names()

    for collection in collections:
        logger.info(f"Processing collection: {collection}")

        # Stream documents to avoid OOM
        cursor = client.extracted[collection].find(batch_size=2000)

        # Read first batch to determine schema
        first_batch = list(cursor.limit(2000))

        if not first_batch:
            continue

        df = pd.DataFrame(first_batch)

        # Drop MongoDB ID column
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        clean_cols = [clean_column(col) for col in df.columns]
        df.columns = clean_cols

        # Create SQL table
        create_cols_sql = ", ".join([f'"{c}" {COLUMN_TYPE_MAPPING[c]}' for c in clean_cols if c in COLUMN_TYPE_MAPPING.keys()] + ['"date" INTEGER'])
        cur.execute(f'CREATE TABLE IF NOT EXISTS REVENUE ({create_cols_sql});')

        # Prepare column list for COPY
        col_list_sql = ", ".join([f'"{c}"' for c in clean_cols if c in COLUMN_TYPE_MAPPING.keys()] + ['"date"'])

        # COPY buffer reusable object
        def copy_rows(batch: pd.DataFrame):
            csv_buffer = io.StringIO()
            
            batch["nombre_de_foyers_fiscaux"] = pd.to_numeric(batch["nombre_de_foyers_fiscaux"], errors='coerce', downcast='integer')
            batch["nombre_de_foyers_fiscaux"] = batch["nombre_de_foyers_fiscaux"].astype('Int64')
            batch["revenu_fiscal_de_reference_des_foyers_fiscaux"] = pd.to_numeric(batch["revenu_fiscal_de_reference_des_foyers_fiscaux"], errors='coerce', downcast='float')
            batch["impot_net_total"] = pd.to_numeric(batch["impot_net_total"], errors='coerce', downcast='float')
            batch["nombre_de_foyers_fiscaux_imposes"] = pd.to_numeric(batch["nombre_de_foyers_fiscaux_imposes"], errors='coerce', downcast='integer')
            batch["nombre_de_foyers_fiscaux_imposes"] = batch["nombre_de_foyers_fiscaux_imposes"].astype('Int64')
            batch["revenu_fiscal_de_reference_des_foyers_fiscaux_imposes"] = pd.to_numeric(batch["revenu_fiscal_de_reference_des_foyers_fiscaux_imposes"], errors='coerce', downcast='float')
            
            def safe_str(val):
                if pd.isna(val) or val == "":
                    return None
                return str(val)
                
            batch['dep'] = batch['dep'].apply(safe_str)
            batch['commune'] = batch['commune'].apply(lambda x: int(x)).apply(safe_str)
            batch['libelle_de_la_commune'] = batch['libelle_de_la_commune'].apply(safe_str)
            batch['revenu_fiscal_de_reference_par_tranche_en_euros'] = batch['revenu_fiscal_de_reference_par_tranche_en_euros'].apply(safe_str).apply(lambda x: x.upper().strip() if x is not None else x)
            batch['date'] = int(collection.split("_")[1])

            batch = batch[batch['revenu_fiscal_de_reference_par_tranche_en_euros'] == 'TOTAL']

            batch = batch.replace({np.nan: None})

            batch = batch[[c for c in clean_cols if c in COLUMN_TYPE_MAPPING.keys()] + ['date']]

            batch.to_csv(csv_buffer, index=False, header=False, na_rep='')

            csv_buffer.seek(0)
            cur.copy_expert(
                f'COPY REVENUE ({col_list_sql}) FROM STDIN WITH CSV',
                csv_buffer
            )
            conn.commit()

        # Send first batch
        copy_rows(df)

        # Stream remaining batches
        batch = []
        for doc in cursor:
            batch.append(doc)

            if len(batch) >= 2000:
                copy_rows(pd.DataFrame(batch))
                batch = []

        # Last incomplete batch
        if batch:
            copy_rows(pd.DataFrame(batch))

## Staging DAG definition

with DAG(
    dag_id="staging_dag",
    description="Staging data pipeline. Moves raw data from MongoDB to staging Postgres.",
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

    revenue_mongo_to_postgres = PythonOperator(
        task_id="revenue_mongo_to_postgres",
        python_callable=revenue_mongo_to_postgres,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )


    start >> revenue_mongo_to_postgres >> end
