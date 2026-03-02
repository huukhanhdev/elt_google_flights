"""
dags/flight_pipeline_dag.py

Daily flight price ELT pipeline.

Flow:
    extract_and_load  →  dbt_run_staging  →  dbt_run_mart  →  dbt_test
"""

import logging
import sys
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Ensure project root is on the path inside the Airflow container
sys.path.insert(0, "/opt/airflow")

from extraction.client import SerpApiClient
from extraction.config import LOOKAHEAD_DAYS, ROUTES
from extraction.loader import PostgresLoader
from monitoring.alert_hooks import notify_on_failure

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Default DAG arguments
# ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_on_failure,
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"


# ─────────────────────────────────────────────────────────────
# Task: Extract & Load
# ─────────────────────────────────────────────────────────────
def extract_and_load(**context) -> None:
    """
    Pulls flight data from SerpApi for all configured routes and
    loads raw JSON into raw.flight_searches (idempotent upsert).
    """
    run_date: date = context["data_interval_start"].date()
    logger.info("Starting extract_and_load for run_date=%s", run_date)

    client = SerpApiClient()

    with PostgresLoader() as loader:
        records_to_fetch = []

        # ── Incremental guard: skip already-loaded route-date combos ──
        for origin, destination in ROUTES:
            for days_ahead in LOOKAHEAD_DAYS:
                departure_date = run_date + timedelta(days=days_ahead)
                if loader.already_loaded(origin, destination, departure_date, run_date):
                    logger.info(
                        "Skipping %s→%s on %s (already loaded for run_date=%s)",
                        origin, destination, departure_date, run_date,
                    )
                else:
                    records_to_fetch.append((origin, destination, departure_date))

        if not records_to_fetch:
            logger.info("All routes already loaded for run_date=%s. Nothing to do.", run_date)
            return

        # ── Fetch from SerpApi ──
        raw_records = []
        for origin, destination, departure_date in records_to_fetch:
            result = client._fetch_single_route(origin, destination, departure_date, run_date)
            if result:
                raw_records.append(result)

        # ── Load into Postgres ──
        inserted = loader.upsert_batch(raw_records)
        logger.info("extract_and_load complete: %d records inserted for run_date=%s", inserted, run_date)


# ─────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="flight_price_pipeline",
    description="Daily ELT pipeline: SerpApi → PostgreSQL raw → dbt staging → dbt mart",
    schedule_interval="0 8 * * *",   # every day at 08:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["flight", "elt", "serpapi"],
) as dag:

    # ── Task 1: Extract from SerpApi, Load into raw layer ──
    t_extract_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )

    # ── Task 2: dbt run — staging layer ──
    t_dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select staging --profiles-dir {DBT_PROFILES_DIR} --no-version-check"
        ),
    )

    # ── Task 3: dbt run — mart layer ──
    t_dbt_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart --profiles-dir {DBT_PROFILES_DIR} --no-version-check"
        ),
    )

    # ── Task 4: dbt test — data quality checks ──
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --no-version-check"
        ),
    )

    # ── Pipeline dependency chain ──
    t_extract_load >> t_dbt_staging >> t_dbt_mart >> t_dbt_test
