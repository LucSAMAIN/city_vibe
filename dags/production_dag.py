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
            full_date DATE UNIQUE,
            year INTEGER,
            month INTEGER,             -- 1-12
            month_name TEXT,           -- Janvier, Février, etc.
            day INTEGER,               -- 1-31
            day_of_week INTEGER,       -- 1=Lundi, 7=Dimanche
            day_name TEXT,             -- Lundi, Mardi, etc.
            is_weekend BOOLEAN
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

    cur.execute("""
        INSERT INTO DIM_DATE (
            date_id, full_date, year, month, month_name, day, day_of_week, day_name, is_weekend
        ) OVERRIDING SYSTEM VALUE VALUES (
            '-1'::BIGINT,  -- date_id
            NULL,   -- full_date
            NULL,   -- year
            NULL,   -- month
            NULL,   -- month_name
            NULL,   -- day
            NULL,   -- day_of_week
            NULL,   -- day_name
            NULL    -- is_weekend
        );                
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

    drop_date_id_from_dvf = SQLExecuteQueryOperator(
        task_id="drop_date_id_from_dvf",
        conn_id="postgres_instance",
        sql="""
            ALTER TABLE DVF_STAGING DROP COLUMN IF EXISTS date_id;
        """,
        dag=dag,
    )

    create_date_id_column_in_dvf = SQLExecuteQueryOperator(
        task_id="create_date_id_column_in_dvf",
        conn_id="postgres_instance",
        sql="""
            ALTER TABLE DVF_STAGING ADD COLUMN IF NOT EXISTS date_id BIGINT;
        """,
        dag=dag,
    )

    create_index = SQLExecuteQueryOperator(
        task_id="create_index",
        conn_id="postgres_instance",
        sql="""
            CREATE INDEX IF NOT EXISTS idx_dvf_full_date ON DIM_DATE(full_date);
        """,
        dag=dag,
    )

    link_dvf_to_date = SQLExecuteQueryOperator(
        task_id="link_dvf_to_date",
        conn_id="postgres_instance",
        sql="""
            UPDATE DVF_STAGING dvf
            SET date_id = subquery.date_id
            FROM (
                SELECT 
                    dvf.id_mutation,
                    dim.date_id
                FROM DVF_STAGING dvf
                LEFT JOIN LATERAL (
                    SELECT date_id
                    FROM DIM_DATE dim
                    WHERE 
                        dim.full_date = dvf.date_mutation
                    LIMIT 1
                ) dim ON TRUE
                WHERE dim.date_id IS NOT NULL
            ) AS subquery
            WHERE dvf.id_mutation = subquery.id_mutation;
        """,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="none_failed",
    )

    # Task dependencies

    start >> dvf_create_dim_date >> drop_date_id_from_dvf >> create_date_id_column_in_dvf >> create_index >> link_dvf_to_date >> end

    

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

    create_dim_building_metrics = SQLExecuteQueryOperator(
        task_id="create_dim_building_metrics",
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
                DATE_TRUNC('year', date_etablissement_dpe) as date_reference,
                
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

            -- INSERTION DE LA LIGNE PAR DÉFAUT (-1)
            INSERT INTO DIM_BUILDING (
                building_id, address_key, date_reference, insee_code, city_name, 
                postal_code, department_num, street_number, gas_emissions, energy_consumption, nb_logements_agg
            ) OVERRIDING SYSTEM VALUE VALUES (
                '-1',   -- building_id
                NULL,   -- address_key
                NULL,   -- date_reference
                NULL,   -- insee_code
                NULL,   -- city_name
                NULL,   -- postal_code
                NULL,   -- department_num
                NULL,   -- street_number
                NULL,   -- gas_emissions
                NULL,   -- energy_consumption
                0       -- nb_logements_agg (0 car aucun logement réel)
            );
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
                        AND dim.date_reference <= DATE_TRUNC('year', dvf.date_mutation)
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
        trigger_rule="none_failed",
    )

    # Task dependencies
    start >> delete_dim_building_table >> create_dim_building_metrics >> populate_dim_building >> create_dim_building_lookup_index >> drop_label_columns_if_exist >> create_label_columns >> update_dim_building_labels >> drop_building_id_from_dvf >> create_building_id_column_in_dvf >> link_dvf_to_building >> create_index_on_dvf_building_id >> end

with DAG(
    dag_id="production-Fact-table",
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

    delete_fact_table = SQLExecuteQueryOperator(
        task_id="delete_fact_table",
        conn_id="postgres_instance",
        sql="""
            DROP TABLE IF EXISTS FACT_TABLE; 
        """,
        dag=dag,
    )

    create_fact_table = SQLExecuteQueryOperator(
        task_id="create_fact_table",
        conn_id="postgres_instance",
        sql="""
            CREATE TABLE IF NOT EXISTS FACT_TABLE (
                fact_id SERIAL PRIMARY KEY,
                id_transaction TEXT,
                date_id BIGINT,       -- FK vers DIM_DATE (Format YYYYMMDD)
                revenue_id BIGINT,    -- FK vers DIM_REVENUE (Key composite)
                building_id BIGINT,   -- FK vers DIM_BUILDING (MD5)
                
                transaction_value FLOAT,
                land_surface FLOAT,
                built_surface FLOAT,
                price_m2 FLOAT,
                transaction_type TEXT
            );
        """,
        dag=dag,
    )

    populate_fact_table = SQLExecuteQueryOperator(
        task_id="populate_fact_table",
        conn_id="postgres_instance",
        sql="""
            TRUNCATE TABLE FACT_TABLE;

            INSERT INTO FACT_TABLE (
                id_transaction, date_id, -- revenue_id, 
                building_id,
                transaction_value, land_surface, built_surface, price_m2, transaction_type
            )
            SELECT
                id_mutation,
                -- 1. DATE_ID : Si la date est nulle -> '-1'
                COALESCE(date_id, '-1'),

                -- 2. REVENUE_ID : Si la clé de jointure est nulle -> '-1'
                -- COALESCE(revenue_id, '-1'),
                
                -- 3. BUILDING_ID : Si le mapping DPE n'a pas marché -> '-1'
                COALESCE(building_id, '-1'),
                
                -- METRIQUES
                valeur_fonciere::FLOAT,
                surface_terrain::FLOAT,
                surface_reelle_bati::FLOAT,
                
                -- PRIX AU M2 (Gestion Division par Zéro)
                CASE 
                    WHEN surface_reelle_bati IS NULL OR surface_reelle_bati = 0 THEN NULL
                    ELSE (valeur_fonciere / surface_reelle_bati)::FLOAT
                END,
                
                -- TYPE
                nature_mutation

            FROM DVF_STAGING

            -- On peut filtrer ici si tu veux exclure les transactions sans valeur
            WHERE id_mutation IS NOT NULL;

            -- 4. Index sur les Clés Étrangères (Indispensable pour la performance des Dashboards)
            CREATE INDEX IF NOT EXISTS idx_fact_date ON FACT_TABLE(date_id);
            CREATE INDEX IF NOT EXISTS idx_fact_rev ON FACT_TABLE(revenue_id);
            CREATE INDEX IF NOT EXISTS idx_fact_build ON FACT_TABLE(building_id);
        """,
        dag=dag,
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="none_failed",
    )

    # Task dependencies
    start >> delete_fact_table >> create_fact_table >> populate_fact_table >> end
