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
REVENUE_TYPE_MAPPING = {
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
    "id_mutation": "TEXT", # Unique transaction identifier
    "date_mutation": "DATE", # Transaction date -> DIM_DATE
    "nature_mutation": "TEXT", # Type of transaction (Vente, etc.)
    "valeur_fonciere": "FLOAT", # Transaction value -> TRANSACTION_FACT.transaction_value
    "code_postal": "TEXT", # Postal code -> DIM_INFO_COMMUNE
    "code_commune": "TEXT", # INSEE code -> DIM_INFO_COMMUNE.insee_code
    "nom_commune": "TEXT", # City name -> DIM_INFO_COMMUNE.city_name
    "code_departement": "TEXT", # Department -> DIM_INFO_COMMUNE.department_num
    "code_type_local": "INTEGER", # Building type code
    "type_local": "TEXT", # Building type -> DIM_BUILDING.type
    "surface_reelle_bati": "FLOAT", # Built surface -> TRANSACTION_FACT.built_surface
    "nombre_pieces_principales": "INTEGER", # Number of rooms (useful for analysis)
    "surface_terrain": "FLOAT", # Land surface -> TRANSACTION_FACT.land_surface
    "longitude": "FLOAT", # Geolocation -> DIM_BUILDING.long
    "latitude": "FLOAT", # Geolocation -> DIM_BUILDING.lat
    "adresse_numero" : "TEXT", # Street number -> DIM_BUILDING.street_number
    "adresse_nom_voie" : "TEXT", # Street name -> DIM_BUILDING.street_name
}

DPE_TYPE_MAPPING = {
    "numero_dpe": "TEXT", # Unique DPE identifier -> DPE_FACT.dpe_id
    "date_etablissement_dpe": "DATE", # Date of diagnosis -> DIM_DATE
    "etiquette_dpe": "TEXT", # Energy consumption label (A-G) -> DPE_FACT.energy_label
    "etiquette_ges": "TEXT", # Greenhouse gas emission label (A-G) -> DPE_FACT.ges_label
    
    "identifiant_ban": "TEXT", # Unique Address ID (BAN) -> DIM_BUILDING.ban_id
    
    "numero_voie_ban": "TEXT", # Street number -> DIM_BUILDING.street_number
    "nom_rue_ban": "TEXT", # Street name -> DIM_BUILDING.street_name
    
    "code_postal_ban": "TEXT", # Postal code -> DIM_INFO_COMMUNE.postal_code
    "code_insee_ban": "TEXT", # INSEE code -> DIM_INFO_COMMUNE.insee_code
    "nom_commune_ban": "TEXT", # City name -> DIM_INFO_COMMUNE.city_name

    "conso_5_usages_par_m2_ep": "FLOAT",      # Consommation énergie primaire
    "emission_ges_5_usages_par_m2": "FLOAT", # Emissions GES
    "type_batiment": "TEXT",
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
def _revenue_mongo_to_postgres():
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
    cur.execute(f'DROP TABLE IF EXISTS REVENUE_STAGING;')

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
        create_cols_sql = ", ".join([f'"{c}" {REVENUE_TYPE_MAPPING[c]}' for c in clean_cols if c in REVENUE_TYPE_MAPPING.keys()] + ['"date" INTEGER'])
        cur.execute(f'CREATE TABLE IF NOT EXISTS REVENUE_STAGING ({create_cols_sql});')

        # Prepare column list for COPY
        col_list_sql = ", ".join([f'"{c}"' for c in clean_cols if c in REVENUE_TYPE_MAPPING.keys()] + ['"date"'])

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
                if pd.isna(val) or val == ""  or val == " "  or val == "  "  or val =="none" \
                    or val == "nan" or val == "NaN" or val == "Nan" or val == "NAN" or val == "nan" \
                    or val == "None" or val == "n.c" or val == "N.C" or val == "n.c." or val == "N.C." \
                    or val == "NULL" or val == "null" or val == ".":
                    return None
                return str(val)
                
            batch['dep'] = batch['dep'].apply(safe_str)
            batch['commune'] = batch['commune'].apply(lambda x: int(x)).apply(safe_str)
            batch['libelle_de_la_commune'] = batch['libelle_de_la_commune'].apply(safe_str)
            batch['revenu_fiscal_de_reference_par_tranche_en_euros'] = batch['revenu_fiscal_de_reference_par_tranche_en_euros'].apply(safe_str).apply(lambda x: x.upper().strip() if x is not None else x)
            batch['date'] = int(collection.split("_")[1])

            batch = batch[batch['revenu_fiscal_de_reference_par_tranche_en_euros'] == 'TOTAL']

            batch = batch.replace({np.nan: None})

            batch = batch[[c for c in clean_cols if c in REVENUE_TYPE_MAPPING.keys()] + ['date']]

            for col in REVENUE_TYPE_MAPPING.keys():
                if col not in batch.columns:
                    batch[col] = None

            batch.to_csv(csv_buffer, index=False, header=False, na_rep='')

            csv_buffer.seek(0)
            cur.copy_expert(
                f'COPY REVENUE_STAGING ({col_list_sql}) FROM STDIN WITH CSV',
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


def _dvf_mongo_to_postgres():
    import psycopg2
    from pymongo import MongoClient
    import pandas as pd
    import numpy as np
    import io

    ARA_DEPARTMENTS = {
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
    logger.info("Dropped existing DVF_STAGING table if it existed")
    
    # Create table with appropriate column types
    create_cols_sql = ", ".join([
        f'"{clean_column(col)}" {DVF_COLUMN_TYPE_MAPPING[col]}' 
        for col in DVF_COLUMN_TYPE_MAPPING.keys()
    ])
    cur.execute(f'CREATE TABLE DVF_STAGING ({create_cols_sql});')
    conn.commit()
    
    logger.info("Created DVF_STAGING table in PostgreSQL")

    # Prepare column list for COPY
    clean_cols = [clean_column(col) for col in DVF_COLUMN_TYPE_MAPPING.keys()]
    col_list_sql = ", ".join([f'"{c}"' for c in clean_cols])

    # Get DVF collection
    collection = client.extracted["dvf"]
    # From doc : Returns the count of all documents in a collection or view.
    total_docs = collection.estimated_document_count()
    logger.info(f"Processing DVF collection with ~{total_docs} documents")

    batch_size = 50000
    processed = 0
    filtered_out = 0

    def normalize_department_code(val):
        """Normalize department code to 2-char string (e.g., '1' -> '01')"""
        if pd.isna(val) or val is None:
            return None
        s = str(val).strip()
        if s.isdigit():
            return s.zfill(2)
        return s.upper()

    def clean_code_5_digits(val):
        """Pour Code Postal et Code INSEE : 01234"""
        if pd.isna(val) or val is None or val == "":
            return None
        try:
            # Gère le cas float : 1234.0 -> 1234 -> "1234" -> "01234"
            return str(int(float(val))).zfill(5)
        except:
            # Gère le cas string sale
            return str(val).strip().zfill(5)

    def clean_street_num(val):
        """Pour numéro de voie : 10, 10 BIS..."""
        if pd.isna(val) or val is None:
            return None
        try:
            # Si c'est un nombre pur (10.0 -> "10")
            return str(int(float(val)))
        except:
            # Si c'est alphanumérique (10 BIS)
            return str(val).strip()

    def copy_rows(batch_df: pd.DataFrame):
        """Clean and copy a batch of rows to PostgreSQL"""
        nonlocal processed, filtered_out
        
        # Keep only relevant columns (handle missing columns gracefully)
        available_cols = [col for col in DVF_COLUMN_TYPE_MAPPING.keys() if col in batch_df.columns]
        batch_df = batch_df[available_cols].copy() # filter
        
        # Add missing columns as None
        for col in DVF_COLUMN_TYPE_MAPPING.keys():
            if col not in batch_df.columns:
                batch_df[col] = None
        
        # Reorder columns
        batch_df = batch_df[DVF_COLUMN_TYPE_MAPPING.keys()]
        
        # Clean column names
        batch_df.columns = clean_cols

        # Normalize department codes and filter for ARA region
        batch_df['code_departement'] = batch_df['code_departement'].apply(normalize_department_code)
        batch_df['code_postal'] = batch_df['code_postal'].apply(clean_code_5_digits)
        batch_df['code_commune'] = batch_df['code_commune'].apply(clean_code_5_digits)
        batch_df['adresse_numero'] = batch_df['adresse_numero'].apply(clean_street_num)

        initial_count = len(batch_df)
        batch_df = batch_df[batch_df['code_departement'].isin(ARA_DEPARTMENTS)]
        filtered_out += (initial_count - len(batch_df))

        
        # Data cleaning and type conversion
        # Filter only sales ("Vente") - we dont care about gifts
        if 'nature_mutation' in batch_df.columns:
            batch_df = batch_df[batch_df['nature_mutation'].isin(['Vente', "Vente en l'état futur d'achèvement"])]
        
        # Convert numeric columns
        batch_df['valeur_fonciere'] = pd.to_numeric(batch_df['valeur_fonciere'], errors='coerce') 
        # Coerce option will turn invalid parsing into NaN
        batch_df['surface_reelle_bati'] = pd.to_numeric(batch_df['surface_reelle_bati'], errors='coerce')
        batch_df['surface_terrain'] = pd.to_numeric(batch_df['surface_terrain'], errors='coerce')
        batch_df['nombre_pieces_principales'] = pd.to_numeric(batch_df['nombre_pieces_principales'], errors='coerce').astype('Int64')
        batch_df['longitude'] = pd.to_numeric(batch_df['longitude'], errors='coerce')
        batch_df['latitude'] = pd.to_numeric(batch_df['latitude'], errors='coerce')
        batch_df['code_type_local'] = pd.to_numeric(batch_df['code_type_local'], errors='coerce').astype('Int64') # like 2 for appartment, 1 for house, etc.

        # Filter rows with value we really need
        batch_df = batch_df[batch_df['valeur_fonciere'].notna() & (batch_df['valeur_fonciere'] > 0)]
        batch_df = batch_df[batch_df['latitude'].notna() & batch_df['longitude'].notna()]
        
        # Convert date
        batch_df['date_mutation'] = pd.to_datetime(batch_df['date_mutation'], errors='coerce')
        
        # Ensure text columns are strings
        # Hypothese : ca prend bcp de temps de faire ca   
        for col in ['id_mutation', 'nature_mutation', 'nom_commune', 'type_local']:
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
    
    logger.info(f"Processed {processed} DVF records (filtered out {filtered_out} non-ARA)")
    
    # Cleanup
    cur.close()
    conn.close()
    client.close()


def _dvf_filter_maisons_appartments():
    # Filtrer les dvf pour prendre uniquement les maisons et appartement en staging

    # First connect to Postgres
    import psycopg2
    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Then execute filtering SQL
    filter_sql = """
    DELETE FROM DVF_STAGING
    WHERE code_type_local NOT IN ('1', '2'); -- 1: Maison, 2: Appartement
    """ 

    cur.execute(filter_sql)
    deleted_rows = cur.rowcount
    conn.commit()
    logger.info(f"Filtered DVF_STAGING to keep only maisons and appartements, deleted {deleted_rows} rows.")

def _dvf_filter_not_null_addresses():
    # Filtrer les dvf pour prendre enlever les null dans les adresses 

    # First connect to Postgres
    import psycopg2
    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    # Then execute filtering SQL
    filter_sql = """
    DELETE FROM DVF_STAGING
    WHERE adresse_numero IS NULL OR adresse_nom_voie IS NULL OR code_postal IS NULL;
    """ 

    cur.execute(filter_sql)
    deleted_rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Filtered DVF_STAGING to keep not null addresses, deleted {deleted_rows} rows.")

def _create_dim_date():
    """Create DIM_DATE dimension table from DVF date_mutation"""
    import psycopg2
    
    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    
    # Create DIM_DATE table
    cur.execute("""
        DROP TABLE IF EXISTS DIM_DATE;
        
        CREATE TABLE DIM_DATE (
            date_id INTEGER PRIMARY KEY,        -- Format YYYYMMDD (e.g., 20240315)
            full_date DATE NOT NULL UNIQUE,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,             -- 1-12
            month_name TEXT NOT NULL,           -- Janvier, Février, etc.
            day INTEGER NOT NULL,               -- 1-31
            day_of_week INTEGER NOT NULL,       -- 1=Lundi, 7=Dimanche
            day_name TEXT NOT NULL,             -- Lundi, Mardi, etc.
            is_weekend BOOLEAN NOT NULL
        );
    """)
    
    # Populate from DVF distinct dates
    cur.execute("""
        INSERT INTO DIM_DATE (
            date_id, full_date, year, month, month_name,
            day, day_of_week, day_name, is_weekend
        )
        SELECT DISTINCT
            TO_CHAR(date_mutation, 'YYYYMMDD')::INTEGER AS date_id,
            date_mutation AS full_date,
            EXTRACT(YEAR FROM date_mutation)::INTEGER AS year,
            EXTRACT(MONTH FROM date_mutation)::INTEGER AS month,
            TO_CHAR(date_mutation, 'TMMonth') AS month_name,
            EXTRACT(DAY FROM date_mutation)::INTEGER AS day,
            EXTRACT(ISODOW FROM date_mutation)::INTEGER AS day_of_week,
            TO_CHAR(date_mutation, 'TMDay') AS day_name,
            EXTRACT(ISODOW FROM date_mutation) IN (6, 7) AS is_weekend
        FROM DVF_STAGING
        WHERE date_mutation IS NOT NULL
        ORDER BY full_date;
    """)
    
    inserted = cur.rowcount
    conn.commit()
    
    logger.info(f"Created DIM_DATE with {inserted} unique dates")

    cur.close()
    conn.close()


def _dpe_mongo_to_postgres():
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
    ordered_cols = list(DPE_TYPE_MAPPING.keys())
    # --- CRÉATION DE LA TABLE ---
    cur.execute('DROP TABLE IF EXISTS DPE_STAGING;')
    logger.info("Deleting table DPE_STAGING")

    # Création dynamique basée sur le mapping
    create_cols_sql = ", ".join([f'"{col}" {dtype}' for col, dtype in DPE_TYPE_MAPPING.items()])
    cur.execute(f'CREATE TABLE IF NOT EXISTS DPE_STAGING ({create_cols_sql});')
    conn.commit()
    
    logger.info(f"Created table DPE_STAGING with columns: {ordered_cols}")

    # Préparation de la liste des colonnes pour le SQL COPY
    col_list_sql = ", ".join([f'"{c}"' for c in ordered_cols])


    # --- FONCTION DE TRAITEMENT ---
    def copy_rows(batch_df: pd.DataFrame):
        csv_buffer = io.StringIO()

        # Nettoyage des ID Mongo si présents
        if "_id" in batch_df.columns:
            batch_df = batch_df.drop(columns=["_id"])

        # Helpers de nettoyage (inclus dans le scope)
        def safe_str(val):
            if pd.isna(val) or val is None: return None
            s_val = str(val).strip()
            if s_val in ["", "none", "nan", "NaN", "NULL", "null", ".", "n.c"]: return None
            return s_val
        
        def clean_code(val):
            if pd.isna(val): return None
            try:
                return str(int(float(val)))
            except:
                return safe_str(val)

        # --- BOUCLE DE NETTOYAGE PRINCIPALE ---
        for col_name, col_type in DPE_TYPE_MAPPING.items():
            
            # 1. Si la colonne n'existe pas dans le batch, on la crée vide
            if col_name not in batch_df.columns:
                batch_df[col_name] = None
                continue # On passe, pas besoin de convertir du None

            # 2. Application du typage
            if col_type == "DATE":
                batch_df[col_name] = pd.to_datetime(batch_df[col_name], errors='coerce')

            elif col_type == "FLOAT":
                # Cela gérera automatiquement vos nouvelles colonnes conso et emission
                batch_df[col_name] = pd.to_numeric(batch_df[col_name], errors='coerce', downcast='float')

            elif col_type == "INTEGER":
                batch_df[col_name] = pd.to_numeric(batch_df[col_name], errors='coerce').astype('Int64')

            elif col_type == "TEXT":
                # Cas spécifiques (Codes postaux, etc.)
                if "code_" in col_name or "identifiant_" in col_name or "numero_" in col_name:
                    batch_df[col_name] = batch_df[col_name].apply(clean_code)
                else:
                    # Cas général (inclut type_batiment)
                    batch_df[col_name] = batch_df[col_name].apply(safe_str)
                
                # Uppercase pour les étiquettes
                if "etiquette" in col_name:
                    batch_df[col_name] = batch_df[col_name].str.upper()


        batch_df = batch_df.replace({np.nan: None})
        
        final_df = batch_df[ordered_cols]

        final_df.to_csv(csv_buffer, index=False, header=False, sep=',', na_rep='')
        csv_buffer.seek(0)

        try:
            cur.copy_expert(
                f"COPY DPE_STAGING ({col_list_sql}) FROM STDIN WITH (FORMAT CSV, NULL '')",
                csv_buffer
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during COPY: {e}")
            raise e

    # --- EXECUTION DU FLUX ---
    batch_size = 50000
    cursor = client.extracted["dpe"].find(batch_size=batch_size)
    processed = 0
    
    batch_list = []
    
    for doc in cursor:
        batch_list.append(doc)

        if len(batch_list) >= batch_size:
            copy_rows(pd.DataFrame(batch_list))
            processed += len(batch_list)
            logger.info(f"Processed {processed} DPE records")
            batch_list = [] # Reset

    # Dernier batch incomplet
    if batch_list:
        copy_rows(pd.DataFrame(batch_list))
        processed += len(batch_list)

    logger.info(f"Finally processed {processed} DPE records total")
    
    # Cleanup
    cur.close()
    conn.close()
    client.close()

def _dpe_filter_maisons_appartments():
    # Filtrer les dpe pour prendre uniquement les maisons et appartement en staging

    # First connect to Postgres
    import psycopg2
    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Then execute filtering SQL
    filter_sql = """
    DELETE FROM DPE_STAGING
    WHERE type_batiment NOT IN ('maison', 'appartement');
    """ 

    cur.execute(filter_sql)
    deleted_rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Filtered DPE_STAGING to keep only maisons and appartements, deleted {deleted_rows} rows.")

def _dpe_filter_not_null_addresses():
    # Filtrer les dpe pour prendre enlever les null dans les adresses 

    # First connect to Postgres
    import psycopg2
    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Then execute filtering SQL
    filter_sql = """
    DELETE FROM DPE_STAGING
    WHERE numero_voie_ban IS NULL OR nom_rue_ban IS NULL OR code_postal_ban IS NULL;
    """ 

    cur.execute(filter_sql)
    deleted_rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Filtered DPE_STAGING to keep not null addresses, deleted {deleted_rows} rows.")

def _dpe_normalize_streets():
    import psycopg2

    STREET_MAPPING = {
        "ALLEE": "ALL",
        "AVENUE": "AV",
        "BOULEVARD": "BD",
        "CHEMIN": "CHE",
        "IMPASSE": "IMP",
        "PLACE": "PL",
        "ROUTE": "RTE",
        "RUE": "RUE",
        "SQUARE": "SQ",
        # "SAINT": "ST",
        # "SAINTE": "STE",
        # "GENERAL": "GAL",
        # "MARECHAL": "MAL",
        # "FAUBOURG": "FG",
        # "PASSAGE": "PAS",
        # "QUAI": "QU",
        # "RESIDENCE": "RES",
        # "MONTEE": "MTE",
        # "COTE": "COT",
        # "CLOS": "CLOS" # Pas d'abréviation standard, mais on le garde pour l'exemple
    }

    # Connexion Postgres
    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    logger.info("Début de la normalisation des adresses via SQL...")

    try:

        # 1. TRANSLATE : On remplace é->e, à->a, etc.
        # 2. UPPER : On met tout en majuscule
        # 3. TRIM : On enlève les espaces inutiles au début/fin
        
        cur.execute("""
            UPDATE DPE_STAGING 
            SET nom_rue_ban = UPPER(TRIM(
                TRANSLATE(
                    nom_rue_ban, 
                    'ÀÂÄÈÉÊËÎÏÔÖÙÛÜÇàâäèéêëîïôöùûüçñÑ', 
                    'AAAEEEEIIOOUUUCaaaeeeeiioouuucnN'
                )
            ))
        """)
        
        logger.info("Accents supprimés et mise en majuscule terminée.")

        # On utilise REGEXP_REPLACE avec \y qui signifie "début ou fin de mot"
        # Cela empêche de remplacer "AUTOROUTE" par "AUTORTE" quand on remplace "ROUTE"
        
        for full_word, abbr in STREET_MAPPING.items():
            if full_word == abbr:
                continue

            # La requête SQL :
            # Remplace le mot entier 'AVENUE' par 'AV'
            # Le flag 'g' signifie global (si le mot apparait 2 fois)
            sql_query = f"""
                UPDATE DPE_STAGING
                SET nom_rue_ban = REGEXP_REPLACE(nom_rue_ban, '\\y{full_word}\\y', '{abbr}', 'g')
                WHERE nom_rue_ban LIKE '%{full_word}%'; 
            """
            # Note: le WHERE LIKE optimise pour ne toucher que les lignes concernées
            
            cur.execute(sql_query)
        
        # ÉTAPE 3 : Nettoyage des articles courants (Optionnel mais recommandé pour les jointures)
        # Ex: "RTE DE LA GLIAT" -> "RTE GLIAT" ? 
        # cur.execute("UPDATE DPE_STAGING SET nom_rue_ban = REGEXP_REPLACE(nom_rue_ban, '\\y(DE|LA|DU|DES|LE|LES)\\y', '', 'g');")
        # cur.execute("UPDATE DPE_STAGING SET nom_rue_ban = TRIM(REGEXP_REPLACE(nom_rue_ban, '\s+', ' ', 'g'));") # Nettoie les doubles espaces créés

        conn.commit()
        logger.info("Normalisation des adresses terminée avec succès.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erreur SQL lors de la normalisation : {e}")
        raise e
    finally:
        cur.close()
        conn.close()

## Staging DAG definition

with DAG(
    dag_id="staging-Revenu",
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
        python_callable=_revenue_mongo_to_postgres,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="none_failed",
    )

    start >> revenue_mongo_to_postgres >> end

with DAG(
    dag_id="staging-Dvf",
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

    dvf_mongo_to_postgres = PythonOperator(
        task_id="dvf_mongo_to_postgres",
        python_callable=_dvf_mongo_to_postgres,
        dag=dag,
    )

    dvf_filtering_null_addresses = PythonOperator(
        task_id="dvf_filtering_null_addresses",
        python_callable=_dvf_filter_not_null_addresses,
        dag=dag,
    )

    dvf_filtering_local_type = PythonOperator(
        task_id="dvf_filtering_local_type",
        python_callable=_dvf_filter_maisons_appartments,
        dag=dag,
    )

    dvf_create_dim_date = PythonOperator(
        task_id="dvf_create_dim_date",
        python_callable=_create_dim_date,
        dag=dag,
    )


    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="none_failed",
    )

    start >> dvf_mongo_to_postgres >> dvf_filtering_local_type >> dvf_filtering_null_addresses >> dvf_create_dim_date >> end

with DAG(
    dag_id="staging-Dpe",
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

    dpe_mongo_to_postgres = PythonOperator(
        task_id="dpe_mongo_to_postgres",
        python_callable=_dpe_mongo_to_postgres,
        dag=dag,
    )

    dpe_filtering_local_type = PythonOperator(
        task_id="dpe_filtering_local_type",
        python_callable=_dpe_filter_maisons_appartments,
        dag=dag,
    )

    dpe_filtering_null_addresses = PythonOperator(
        task_id="dpe_filtering_null_addresses",
        python_callable=_dpe_filter_not_null_addresses,
        dag=dag,
    )

    dpe_normalize_addresses = PythonOperator(
        task_id="dpe_normalize_addresses",
        python_callable=_dpe_normalize_streets,
        dag=dag,
    )


    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="none_failed",
    )

    start >> dpe_mongo_to_postgres >> dpe_filtering_local_type >> dpe_filtering_null_addresses >> dpe_normalize_addresses >> end


