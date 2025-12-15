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


DVF_COLUMN_TYPE_MAPPING = {
    "id_mutation": "TEXT",
    "date_mutation": "DATE",
    "nature_mutation": "TEXT",
    "valeur_fonciere": "FLOAT",
    "code_postal": "TEXT",
    "code_commune": "TEXT",
    "nom_commune": "TEXT",
    "code_departement": "TEXT",
    "code_type_local": "TEXT",
    "type_local": "TEXT",
    "surface_reelle_bati": "FLOAT",
    "nombre_pieces_principales": "INTEGER",
    "surface_terrain": "FLOAT",
    "longitude": "FLOAT",
    "latitude": "FLOAT",
}

# Columns to keep from DVF dataset (relevant for our analysis)
DVF_COLUMNS_TO_KEEP = [
    "id_mutation",        # Unique transaction identifier
    "date_mutation",      # Transaction date -> DIM_DATE
    "nature_mutation",    # Type of transaction (Vente, etc.)
    "valeur_fonciere",    # Transaction value -> TRANSACTION_FACT.transaction_value
    "code_postal",        # Postal code -> DIM_INFO_COMMUNE
    "code_commune",       # INSEE code -> DIM_INFO_COMMUNE.insee_code
    "nom_commune",        # City name -> DIM_INFO_COMMUNE.city_name
    "code_departement",   # Department -> DIM_INFO_COMMUNE.department_num
    "code_type_local",    # Building type code
    "type_local",         # Building type -> DIM_BUILDING.type
    "surface_reelle_bati",# Built surface -> TRANSACTION_FACT.built_surface
    "nombre_pieces_principales",  # Number of rooms (useful for analysis)
    "surface_terrain",    # Land surface -> TRANSACTION_FACT.land_surface
    "longitude",          # Geolocation -> DIM_BUILDING.long
    "latitude",           # Geolocation -> DIM_BUILDING.lat
]

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
        if not collection.startswith("revenue_"):
            continue

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


def dvf_mongo_to_postgres():
    import psycopg2
    from pymongo import MongoClient
    import pandas as pd
    import numpy as np
    import io

    # Connect to MongoDB
    client = MongoClient(
        "mongodb://mongo:27017/",
        username="admin",
        password="admin",
        authSource="admin"
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
    
    # Drop and recreate DVF staging table
    cur.execute('DROP TABLE IF EXISTS DVF_STAGING;')
    
    # Create table with appropriate column types
    create_cols_sql = ", ".join([
        f'"{clean_column(col)}" {DVF_COLUMN_TYPE_MAPPING[col]}' 
        for col in DVF_COLUMNS_TO_KEEP
    ])
    cur.execute(f'CREATE TABLE IF NOT EXISTS DVF_STAGING ({create_cols_sql});')
    conn.commit()
    
    logger.info("Created DVF_STAGING table in PostgreSQL")

    # Prepare column list for COPY
    clean_cols = [clean_column(col) for col in DVF_COLUMNS_TO_KEEP]
    col_list_sql = ", ".join([f'"{c}"' for c in clean_cols])

    # Get DVF collection
    collection = client.extracted["dvf"]
    # From doc : Returns the count of all documents in a collection or view.
    total_docs = collection.estimated_document_count()
    logger.info(f"Processing DVF collection with ~{total_docs} documents")

    batch_size = 100000
    processed = 0

    def copy_rows(batch_df: pd.DataFrame):
        """Clean and copy a batch of rows to PostgreSQL"""
        nonlocal processed
        
        # Keep only relevant columns (handle missing columns gracefully)
        available_cols = [col for col in DVF_COLUMNS_TO_KEEP if col in batch_df.columns]
        batch_df = batch_df[available_cols].copy() # filter
        
        # Add missing columns as None
        for col in DVF_COLUMNS_TO_KEEP:
            if col not in batch_df.columns:
                batch_df[col] = None
        
        # Reorder columns
        batch_df = batch_df[DVF_COLUMNS_TO_KEEP]
        
        # Clean column names
        batch_df.columns = clean_cols
        
        # Data cleaning and type conversion
        # Filter only sales ("Vente") - we dont care about gifts
        if 'nature_mutation' in batch_df.columns:
            batch_df = batch_df[batch_df['nature_mutation'].isin(['Vente', "Vente en l'état futur d'achèvement"])]
        
        # Filter rows with value we really need
        batch_df = batch_df[batch_df['valeur_fonciere'].notna() & (batch_df['valeur_fonciere'] > 0)]
        batch_df = batch_df[batch_df['latitude'].notna() & batch_df['longitude'].notna()]
        
        # Convert numeric columns
        batch_df['valeur_fonciere'] = pd.to_numeric(batch_df['valeur_fonciere'], errors='coerce')
        batch_df['surface_reelle_bati'] = pd.to_numeric(batch_df['surface_reelle_bati'], errors='coerce')
        batch_df['surface_terrain'] = pd.to_numeric(batch_df['surface_terrain'], errors='coerce')
        batch_df['nombre_pieces_principales'] = pd.to_numeric(batch_df['nombre_pieces_principales'], errors='coerce').astype('Int64')
        batch_df['longitude'] = pd.to_numeric(batch_df['longitude'], errors='coerce')
        batch_df['latitude'] = pd.to_numeric(batch_df['latitude'], errors='coerce')
        
        # Convert date
        batch_df['date_mutation'] = pd.to_datetime(batch_df['date_mutation'], errors='coerce')
        
        # Ensure text columns are strings
        for col in ['id_mutation', 'nature_mutation', 'code_postal', 'code_commune', 
                    'nom_commune', 'code_departement', 'code_type_local', 'type_local']:
            if col in batch_df.columns:
                batch_df[col] = batch_df[col].astype(str).replace('nan', '')
        
        # Replace NaN with None for proper NULL handling
        batch_df = batch_df.replace({np.nan: None, 'nan': None, '': None})
        
        if batch_df.empty:
            return # after filtering this batch is empty lol
        
        # Write to CSV buffer
        csv_buffer = io.StringIO() 
        batch_df.to_csv(csv_buffer, index=False, header=False, na_rep='') # convert df to csv string
        csv_buffer.seek(0) # rewind to the start
        
        # COPY to PostgreSQL (way faster, we need a csv buffer beforehand tho)
        cur.copy_expert(
            f'COPY DVF_STAGING ({col_list_sql}) FROM STDIN WITH CSV NULL \'\'',
            csv_buffer
        )
        conn.commit()
        
        processed += len(batch_df)
        logger.info(f"Processed {processed} DVF records")

    # Stream documents from MongoDB in batches
    cursor = client.extracted["dvf"].find(batch_size=batch_size)
    batch = []
    
    for doc in cursor:
        # Remove MongoDB _id field
        doc.pop('_id', None)
        batch.append(doc)
        
        if len(batch) >= batch_size: # so we only do it for every batch size documents
            copy_rows(pd.DataFrame(batch))
            batch = []
    
    # Process remaining batch
    if batch:
        copy_rows(pd.DataFrame(batch))
    
    # Create indexes for better query performance
    # logger.info("Creating indexes on DVF_STAGING table...")
    # cur.execute('CREATE INDEX IF NOT EXISTS idx_dvf_code_commune ON DVF_STAGING (code_commune);')
    # cur.execute('CREATE INDEX IF NOT EXISTS idx_dvf_date_mutation ON DVF_STAGING (date_mutation);')
    # cur.execute('CREATE INDEX IF NOT EXISTS idx_dvf_code_departement ON DVF_STAGING (code_departement);')
    # conn.commit()
    
    logger.info(f"DVF staging complete. Total records: {processed}")
    
    # Cleanup
    cur.close()
    conn.close()
    client.close()


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

    dvf_mongo_to_postgres_task = PythonOperator(
        task_id="dvf_mongo_to_postgres",
        python_callable=dvf_mongo_to_postgres,
        dag=dag,
    )


    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )

    start >> [revenue_mongo_to_postgres, dvf_mongo_to_postgres_task] >> end
