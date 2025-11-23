from datetime import datetime, timedelta
import os

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

def clean_value(v):
    # Convert Mongo ObjectId or any object → string
    if not isinstance(v, (int, float, str)) and v is not None:
        v = str(v)

    # NULL handling
    if v is None:
        return "NULL"

    # Numeric values → keep raw
    if isinstance(v, (int, float)):
        return str(v)

    # Strings → escape single quotes
    if isinstance(v, str):
        v = v.replace("'", "''")
        return f"'{v}'"

    # Fallback
    return f"'{str(v)}'"

def _mongo_to_postgres():
    import psycopg2
    from pymongo import MongoClient
    import pandas as pd
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

    collections = client.extracted.list_collection_names()

    for collection in collections:
        print(f"Processing collection: {collection}")

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

        # Create SQL table
        create_cols_sql = ", ".join([f'"{c}" TEXT' for c in clean_cols])
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{collection}" ({create_cols_sql});')

        # Prepare column list for COPY
        col_list_sql = ", ".join([f'"{c}"' for c in clean_cols])

        # COPY buffer reusable object
        def copy_rows(row_batch):
            csv_buffer = io.StringIO()
            for row in row_batch:
                cleaned_row = [clean_value(row.get(orig_col)) for orig_col in df.columns]
                csv_buffer.write(",".join(cleaned_row) + "\n")

            csv_buffer.seek(0)

            cur.copy_expert(
                f'COPY "{collection}" ({col_list_sql}) FROM STDIN WITH CSV',
                csv_buffer
            )
            conn.commit()

        # Send first batch
        copy_rows(first_batch)

        # Stream remaining batches
        batch = []
        for doc in cursor:
            batch.append(doc)

            if len(batch) >= 2000:
                copy_rows(batch)
                batch = []

        # Last incomplete batch
        if batch:
            copy_rows(batch)

with DAG(
    dag_id="production_dag",
    description="Schéma global des pipelines City Vibe (ingestion -> dimensions -> faits -> qualité -> mart)",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    default_args=default_args,
    tags=["city_vibe", "demo"],
    template_searchpath="/opt/airflow/data/sql/"
) as dag:

    start = EmptyOperator(
        task_id="start",
        dag=dag,
    )

    mongo_to_postgres = PythonOperator(
        task_id="mongo_to_postgres",
        python_callable=_mongo_to_postgres,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )


    start >> mongo_to_postgres >> end