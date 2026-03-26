"""
Airflow DAG: Multi-Source News Ingestion

5 tasks, one per source, running hourly:
  factcheck → azattyq → informburo → tengrinews → ztb_ingest_and_verify

The first 4 build the knowledge base (reference sources).
The last task ingests ZTB articles, extracts claims, and verifies them against the KB.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "factchecker",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _ingest_source(source_key: str, **kwargs):
    """Generic task: run ingestion for a single source."""
    from ingestion.news_scraper import run_source_ingestion
    return run_source_ingestion(source_key, months_back=1)


with DAG(
    dag_id="news_ingest",
    default_args=default_args,
    description="Hourly: ingest 4 reference sources + ZTB verification",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["news", "ingestion", "kz", "rag"],
) as dag:

    ingest_factcheck = PythonOperator(
        task_id="ingest_factcheck",
        python_callable=_ingest_source,
        op_args=["factcheck"],
        execution_timeout=timedelta(hours=2),
    )

    ingest_azattyq = PythonOperator(
        task_id="ingest_azattyq",
        python_callable=_ingest_source,
        op_args=["azattyq"],
        execution_timeout=timedelta(hours=2),
    )

    ingest_informburo = PythonOperator(
        task_id="ingest_informburo",
        python_callable=_ingest_source,
        op_args=["informburo"],
        execution_timeout=timedelta(hours=2),
    )

    ingest_tengrinews = PythonOperator(
        task_id="ingest_tengrinews",
        python_callable=_ingest_source,
        op_args=["tengrinews"],
        execution_timeout=timedelta(hours=2),
    )

    ingest_ztb = PythonOperator(
        task_id="ingest_ztb_and_verify",
        python_callable=_ingest_source,
        op_args=["ztb"],
        execution_timeout=timedelta(hours=2),
    )

    # Reference sources first (parallel), then ZTB verification
    [ingest_factcheck, ingest_azattyq, ingest_informburo, ingest_tengrinews] >> ingest_ztb
