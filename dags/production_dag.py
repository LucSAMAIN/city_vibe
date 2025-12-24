from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


import logging

# Function to create DIM_DATE table
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


## Configuration

logger = logging.getLogger(__name__)

default_args = {
    "owner": "city_vibe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _delete_dim_building_table():
    import psycopg2

    conn = psycopg2.connect(
        host="postgres-instance",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS DIM_BUILDING;')


## Production DAG definition

with DAG(
    dag_id="production-Date-building",
    description="Staging to production data pipeline. Moves cleaned data from staging postgres to production postgres and computes metrics.",
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

    dvf_create_dim_date = PythonOperator(
        task_id="dvf_create_dim_date",
        python_callable=_create_dim_date,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )

    # Task dependencies

    start >> dvf_create_dim_date >> end

    

with DAG(
    dag_id="production-Dim-building",
    description="Staging to production data pipeline. Moves cleaned data from staging postgres to production postgres and computes metrics.",
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

    delete_dim_building_table = PythonOperator(
        task_id="delete_dim_building_table",
        python_callable=_delete_dim_building_table,
        dag=dag,
    )

    create_dim_building_metrics = SQLExecuteQueryOperator(
        task_id="create_dim_building_metrics",
        conn_id="postgres_instance",
        sql="""
            CREATE TABLE IF NOT EXISTS DIM_BUILDING (
                building_id TEXT, -- On ne met pas PRIMARY KEY pour l'instant pour éviter les blocages, on gérera via index
                address_key TEXT,
                date_reference DATE,
                insee_code TEXT,
                city_name TEXT,
                postal_code TEXT,
                department_num TEXT,
                street_number TEXT,
                gas_emissions FLOAT,
                energy_consumption FLOAT
            );

            TRUNCATE TABLE DIM_BUILDING;

            INSERT INTO DIM_BUILDING (
                building_id, address_key, date_reference, insee_code, city_name, 
                postal_code, department_num, street_number, gas_emissions, energy_consumption
            )
            SELECT
                md5(address_key || '_' || date_etablissement_dpe::text),
                
                address_key,
                date_etablissement_dpe,
                
                MAX(code_insee_ban),
                MAX(nom_commune_ban),                
                MAX(code_postal_ban),
                LEFT(MAX(code_postal_ban)::TEXT, 2),
                MAX(numero_voie_ban),
                
                AVG(emission_ges_5_usages_par_m2)::FLOAT,
                AVG(conso_5_usages_par_m2_ep)::FLOAT

            FROM DPE_STAGING
            WHERE address_key IS NOT NULL 
              AND date_etablissement_dpe IS NOT NULL
            GROUP BY address_key, date_etablissement_dpe;

            CREATE INDEX IF NOT EXISTS idx_dim_building_lookup ON DIM_BUILDING(address_key, date_reference);
        """,
        dag=dag,
    )

    update_dim_building_labels = SQLExecuteQueryOperator(
        task_id="update_dim_building_labels",
        conn_id="postgres_instance",
        sql="""
            ALTER TABLE DIM_BUILDING ADD COLUMN IF NOT EXISTS gas_emissions_label TEXT;
            ALTER TABLE DIM_BUILDING ADD COLUMN IF NOT EXISTS energy_consumption_label TEXT;

            UPDATE DIM_BUILDING
            SET gas_emissions_label = CASE 
                WHEN gas_emissions <= 6 THEN 'A'
                WHEN gas_emissions <= 11 THEN 'B'
                WHEN gas_emissions <= 30 THEN 'C'
                WHEN gas_emissions <= 50 THEN 'D'
                WHEN gas_emissions <= 70 THEN 'E'
                WHEN gas_emissions <= 100 THEN 'F'
                WHEN gas_emissions > 100 THEN 'G'
                ELSE NULL
            END,
            energy_consumption_label = CASE 
                WHEN energy_consumption <= 70 THEN 'A'
                WHEN energy_consumption <= 110 THEN 'B'
                WHEN energy_consumption <= 180 THEN 'C'
                WHEN energy_consumption <= 250 THEN 'D'
                WHEN energy_consumption <= 330 THEN 'E'
                WHEN energy_consumption <= 420 THEN 'F'
                WHEN energy_consumption > 420 THEN 'G'
                ELSE NULL
            END;
        """,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )

    # Task dependencies
    start >> delete_dim_building_table >> create_dim_building_metrics >> update_dim_building_labels >>  end
