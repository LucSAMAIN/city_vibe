from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# DAG de démonstration: schéma global des pipelines City Vibe
# Ingestion (bronze) -> Dimensions (silver) -> Faits (gold) -> Qualité -> Data mart

default_args = {
    "owner": "city_vibe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="city_vibe_demo",
    description="Schéma global des pipelines City Vibe (ingestion -> dimensions -> faits -> qualité -> mart)",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    default_args=default_args,
    tags=["city_vibe", "demo"],
) as dag:

    end = EmptyOperator(task_id="end")

    # Orchestration globale
    start >> [ingest_dvf, ingest_revenue, ingest_geodata] >> bronze_ready
    bronze_ready >> build_dims
    [bronze_ready, build_dims] >> build_fact
    [build_dims, build_fact] >> dq
    dq >> mart >> end
