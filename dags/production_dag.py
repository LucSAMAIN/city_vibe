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

    end = EmptyOperator(
        task_id="end",
        dag=dag,
        trigger_rule="all_done",
    )


    start >> end