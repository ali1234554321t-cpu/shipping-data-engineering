from datetime import datetime
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/shipping"
PYTHON = "python"

COMMON_ENV = {
    "SHIPPING_DB_HOST": "shipping-postgres",
    "SHIPPING_DB_PORT": "5432",
    "SHIPPING_DB_NAME": "shipping_db",
    "SHIPPING_DB_USER": "shipping_user",
    "SHIPPING_DB_PASSWORD": os.environ["SHIPPING_DB_PASSWORD"],
    "DBT_HOST": "shipping-postgres",
    "DBT_PORT": "5432",
    "DBT_DBNAME": "shipping_db",
    "DBT_USER": "shipping_user",
    "DBT_PASSWORD": os.environ["DBT_PASSWORD"],
    "DBT_PROFILES_DIR": "/opt/shipping/airflow/dbt_profiles",
}

def python_task(task_id, script):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} {script}",
        env=COMMON_ENV,
        append_env=True,
    )

with DAG(
    dag_id="shipping_data_pipeline",
    description="Shipping data engineering pipeline: ingestion, medallion, PostgreSQL, and dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["shipping", "pyspark", "dbt"],
) as dag:

    fetch_port_capacity = python_task(
        "fetch_port_capacity",
        "ingestion/fetch_port_capacity.py",
    )

    bronze_port_capacity = python_task(
        "bronze_port_capacity",
        "processing/bronze_port_capacity.py",
    )

    silver_port_capacity = python_task(
        "silver_port_capacity",
        "processing/silver_port_capacity.py",
    )

    gold_port_capacity = python_task(
        "gold_port_capacity",
        "processing/gold_port_capacity.py",
    )

    bronze_shipment_events = python_task(
        "bronze_shipment_events",
        "processing/bronze_shipment_events.py",
    )

    silver_shipment_events = python_task(
        "silver_shipment_events",
        "processing/silver_shipment_events.py",
    )

    gold_shipment_summary = python_task(
        "gold_shipment_summary",
        "processing/gold_shipment_summary.py",
    )

    load_gold_port_capacity = python_task(
        "load_gold_port_capacity",
        "scripts/load_gold_port_capacity_summary.py",
    )

    load_gold_shipment_summary = python_task(
        "load_gold_shipment_summary",
        "scripts/load_gold_shipment_summary.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/shipping/shipping_analytics && "
            "dbt build --profiles-dir /opt/shipping/airflow/dbt_profiles"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    (
        fetch_port_capacity
        >> bronze_port_capacity
        >> silver_port_capacity
        >> gold_port_capacity
        >> load_gold_port_capacity
    )

    (
        bronze_shipment_events
        >> silver_shipment_events
        >> gold_shipment_summary
        >> load_gold_shipment_summary
    )

    [load_gold_port_capacity, load_gold_shipment_summary] >> dbt_build
