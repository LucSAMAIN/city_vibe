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
            date_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
            full_date, year, month, month_name,
            day, day_of_week, day_name, is_weekend
        )
        SELECT DISTINCT
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

## Production DAG definition

with DAG(
    dag_id="production-Dim-date",
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

    delete_dim_building_table = SQLExecuteQueryOperator(
        task_id="delete_dim_building_table",
        conn_id="postgres_instance",
        sql="""
            DROP TABLE IF EXISTS DIM_BUILDING; 
        """,
        dag=dag,
    )

    create_dim_building_schema = SQLExecuteQueryOperator(
        task_id="create_dim_building_schema",
        conn_id="postgres_instance",
        sql="""
            CREATE TABLE IF NOT EXISTS DIM_BUILDING (
                building_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, 
                address_key TEXT,
                date_reference DATE,
                insee_code TEXT,
                city_name TEXT,
                postal_code TEXT,
                department_num TEXT,
                street_number TEXT,
                street_name TEXT,
                gas_emissions FLOAT,
                energy_consumption FLOAT,
                nb_logements_agg INTEGER
            );
        """,
        dag=dag,
    )

    populate_dim_building = SQLExecuteQueryOperator(
        task_id="populate_dim_building",
        conn_id="postgres_instance",
        sql="""
            INSERT INTO DIM_BUILDING (
                address_key, date_reference, insee_code, city_name, 
                postal_code, department_num, street_number, street_name, gas_emissions, energy_consumption, nb_logements_agg
            )
            SELECT              
                address_key,
                DATE_TRUNC('year', date_etablissement_dpe)::DATE as date_reference,
                
                MAX(code_insee_ban),
                MAX(nom_commune_ban),                
                MAX(code_postal_ban),
                LEFT(MAX(code_postal_ban)::TEXT, 2),
                MAX(numero_voie_ban),
                MAX(nom_rue_ban),
                
                AVG(emission_ges_5_usages_par_m2)::FLOAT,
                AVG(conso_5_usages_par_m2_ep)::FLOAT,
                -- On compte combien d'apparts ont été moyennés (info utile)
                COUNT(*) as nb_logements_agg

            FROM DPE_STAGING
            WHERE address_key IS NOT NULL 
              AND date_etablissement_dpe IS NOT NULL
            GROUP BY address_key, DATE_TRUNC('year', date_etablissement_dpe);
            """,
        dag=dag,
    )

    create_dim_building_lookup_index = SQLExecuteQueryOperator(
        task_id="create_dim_building_lookup_index",
        conn_id="postgres_instance",
        sql="""
            CREATE INDEX IF NOT EXISTS idx_dim_building_lookup ON DIM_BUILDING(address_key, date_reference);
        """,
        dag=dag,
    )

    drop_label_columns_if_exist = SQLExecuteQueryOperator(
        task_id="drop_label_columns_if_exist",
        conn_id="postgres_instance",
        sql="""
            ALTER TABLE DIM_BUILDING DROP COLUMN IF EXISTS gas_emissions_label;
            ALTER TABLE DIM_BUILDING DROP COLUMN IF EXISTS energy_consumption_label;
        """,
        dag=dag,
    )

    create_label_columns = SQLExecuteQueryOperator(
        task_id="create_label_columns",
        conn_id="postgres_instance",
        sql="""
            ALTER TABLE DIM_BUILDING ADD COLUMN IF NOT EXISTS gas_emissions_label TEXT;
            ALTER TABLE DIM_BUILDING ADD COLUMN IF NOT EXISTS energy_consumption_label TEXT;
        """,
        dag=dag,
    )

    update_dim_building_labels = SQLExecuteQueryOperator(
        task_id="update_dim_building_labels",
        conn_id="postgres_instance",
        sql="""
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

    drop_building_id_from_dvf = SQLExecuteQueryOperator(
        task_id="drop_building_id_from_dvf",
        conn_id="postgres_instance",
        sql="""
            ALTER TABLE DVF_STAGING DROP COLUMN IF EXISTS building_id;
        """,
        dag=dag,
    )

    create_building_id_column_in_dvf = SQLExecuteQueryOperator(
        task_id="create_building_id_column_in_dvf",
        conn_id="postgres_instance",
        sql="""
            ALTER TABLE DVF_STAGING ADD COLUMN IF NOT EXISTS building_id BIGINT;
        """,
        dag=dag,
    )

    link_dvf_to_building = SQLExecuteQueryOperator(
        task_id="link_dvf_to_building",
        conn_id="postgres_instance",
        sql="""
            UPDATE DVF_STAGING dvf
            SET building_id = subquery.building_id
            FROM (
                SELECT 
                    dvf.id_mutation,
                    dim.building_id
                FROM DVF_STAGING dvf
                LEFT JOIN LATERAL (
                    SELECT building_id
                    FROM DIM_BUILDING dim
                    WHERE 
                        dim.address_key = dvf.address_key
                        -- On cherche le DPE fait AVANT ou PENDANT la vente
                        AND dim.date_reference <= dvf.date_mutation
                    ORDER BY dim.date_reference DESC
                    LIMIT 1
                ) dim ON TRUE
                WHERE dim.building_id IS NOT NULL
            ) AS subquery
            WHERE dvf.id_mutation = subquery.id_mutation;
        """,
        dag=dag,
    )

    create_index_on_dvf_building_id = SQLExecuteQueryOperator(
        task_id="create_index_on_dvf_building_id",
        conn_id="postgres_instance",
        sql="""
            CREATE INDEX IF NOT EXISTS idx_dvf_building_id ON DVF_STAGING(building_id);
        """,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )

    # Task dependencies
    start >> delete_dim_building_table >> create_dim_building_schema >> populate_dim_building >> create_dim_building_lookup_index >> drop_label_columns_if_exist >> create_label_columns >> update_dim_building_labels >> drop_building_id_from_dvf >> create_building_id_column_in_dvf >> link_dvf_to_building >> create_index_on_dvf_building_id >> end
