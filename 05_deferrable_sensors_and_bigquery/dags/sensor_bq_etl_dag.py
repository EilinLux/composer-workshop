"""
Chapter 05 Reference DAG: Deferrable Sensors, TaskFlow API & BigQuery Checks
============================================================================
Reference Notebook: ../05_deferrable_sensors_and_bigquery.ipynb
Companion Script:   ../scripts/01_deploy_sensor_dag.sh
Chapter Guide:      ../README.md

What this DAG demonstrates:
  1. `wait_for_daily_manifest` (GCSObjectExistenceSensor, deferrable=True):
     Asynchronously waits for an upstream `_SUCCESS` marker object in Cloud Storage
     using the Airflow Triggerer, without holding onto a worker slot.
  2. `wait_for_bq_source_table` (BigQueryTableExistenceSensor, deferrable=True):
     Asynchronously confirms the upstream BigQuery table exists.
  3. `build_query_parameters` (@task):
     Uses the TaskFlow API to construct runtime metadata and push it to XCom.
  4. `aggregate_daily_sales_in_bq` (BigQueryInsertJobOperator):
     Pushes SQL aggregation down to BigQuery, creating a date-partitioned and
     product-clustered summary table.
  5. `validate_output_not_empty` (BigQueryCheckOperator):
     Executes a post-load data quality assertion verifying that at least 1 row
     was written for the target processing date.
"""

from __future__ import annotations

import os
import pendulum

# Load local `.env` variables when validating the DAG locally via `uv run pytest`
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Airflow core decorators for functional DAG and TaskFlow definitions
from airflow.decorators import dag, task

# BigQuery native execution and data-quality assertion operators
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)

# Google Cloud sensors supporting asynchronous Deferrable execution (`deferrable=True`)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# ------------------------------------------------------------------------------
# 1. Configuration Variables (Loaded from .env locally or Airflow Variables)
# ------------------------------------------------------------------------------
# Read fallback defaults from `.env` so the DAG works both locally and in Composer
DEFAULT_PROJECT = os.getenv("GCP_PROJECT_ID", "gai-community29oct-7500")
DEFAULT_DATASET = os.getenv("BQ_DATASET_ID", "example_composer_workshop")
DEFAULT_LANDING_BUCKET = os.getenv("RAW_LANDING_BUCKET", "gai-community29oct-7500-dataproc-temp")
BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")

# Render Airflow Variables dynamically at task runtime using Jinja (`var.value.get(...)`)
GCP_PROJECT_ID = "{{ var.value.get('gcp_project_id', '" + DEFAULT_PROJECT + "') }}"
GCP_DATASET_ID = DEFAULT_DATASET
RAW_LANDING_BUCKET = "{{ var.value.get('raw_landing_bucket', '" + DEFAULT_LANDING_BUCKET + "') }}"


# ------------------------------------------------------------------------------
# 2. Workflow Definition (`@dag`)
# ------------------------------------------------------------------------------
@dag(
    # Unique DAG identifier in the Airflow UI
    dag_id="composer_deferrable_sensors_and_taskflow_etl",
    # Start of logical scheduling window
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Run once daily at midnight UTC
    schedule="@daily",
    # Disable historical backfill on initial deployment
    catchup=False,
    # Organizational tags for filtering in the Airflow Web UI
    tags=["chapter-05", "composer", "sensors", "deferrable", "bigquery", "taskflow"],
    # Default retry settings inherited by all tasks in this DAG
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
    },
)
def deferrable_sensors_and_taskflow_pipeline():
    """End-to-end pipeline combining Deferrable Sensors, TaskFlow API, and BigQuery Checks."""

    # --------------------------------------------------------------------------
    # Task 1a: Deferrable Cloud Storage Object Sensor
    # --------------------------------------------------------------------------
    # Waits for `gs://<bucket>/incoming/sales/<YYYY-MM-DD>/_SUCCESS` to appear.
    # Because `deferrable=True` is enabled, the task immediately yields its worker
    # slot and polls asynchronously inside the Airflow Triggerer process.
    wait_for_manifest = GCSObjectExistenceSensor(
        task_id="wait_for_daily_manifest",
        bucket=RAW_LANDING_BUCKET,
        object="incoming/sales/{{ dag_run.conf.get('processing_date', ds) }}/_SUCCESS",
        deferrable=True,
        # Check every 30 seconds
        poke_interval=30,
        # Fail the sensor if the file has not arrived within 30 minutes (1800 seconds)
        timeout=60 * 30,
        mode="reschedule",
    )

    # --------------------------------------------------------------------------
    # Task 1b: Deferrable BigQuery Table Existence Sensor
    # --------------------------------------------------------------------------
    # Confirms that the upstream `sales_data` table exists in BigQuery before
    # attempting to run downstream aggregation queries.
    wait_for_source_table = BigQueryTableExistenceSensor(
        task_id="wait_for_bq_source_table",
        project_id=GCP_PROJECT_ID,
        dataset_id=GCP_DATASET_ID,
        table_id="sales_data",
        deferrable=True,
    )

    # --------------------------------------------------------------------------
    # Task 2: TaskFlow API Metadata Builder (`@task`)
    # --------------------------------------------------------------------------
    # Computes runtime parameters in Python and serializes the returned dictionary
    # to XCom automatically.
    @task
    def build_query_parameters(logical_date: str | None = None) -> dict[str, str]:
        """Builds target processing date and destination table metadata."""
        target_date = logical_date or os.getenv("DEFAULT_PROCESSING_DATE", "2025-06-12")
        return {
            "target_date": target_date,
            "destination_table": f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.daily_sales_summary",
        }

    # Invoke the `@task` function, passing the logical date or manual override
    query_meta = build_query_parameters(
        logical_date="{{ dag_run.conf.get('processing_date', ds) }}"
    )

    # --------------------------------------------------------------------------
    # Task 3: Native BigQuery SQL Pushdown (`BigQueryInsertJobOperator`)
    # --------------------------------------------------------------------------
    # Pushes the aggregation query down to BigQuery's serverless SQL engine,
    # creating a table partitioned by `processing_date` and clustered by `product_id`.
    aggregate_daily_sales = BigQueryInsertJobOperator(
        task_id="aggregate_daily_sales_in_bq",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.daily_sales_summary`
                    PARTITION BY processing_date
                    CLUSTER BY product_id AS
                    SELECT
                        product_id,
                        ROUND(SUM(amount), 2) AS total_sales,
                        COUNT(1) AS transaction_count,
                        DATE('{{{{ dag_run.conf.get("processing_date", ds) }}}}') AS processing_date
                    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.sales_data`
                    WHERE transaction_date = DATE('{{{{ dag_run.conf.get("processing_date", ds) }}}}')
                    GROUP BY product_id
                """,
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------------------------------
    # Task 4: Post-Load Data Quality Check (`BigQueryCheckOperator`)
    # --------------------------------------------------------------------------
    # Runs a SQL query that must return a single truthy value (`COUNT(1) > 0`).
    # If 0 rows were written for the target date, this task fails and alerts the team.
    validate_output_not_empty = BigQueryCheckOperator(
        task_id="validate_output_not_empty",
        sql=f"""
            SELECT COUNT(1) > 0
            FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.daily_sales_summary`
            WHERE processing_date = DATE('{{{{ dag_run.conf.get("processing_date", ds) }}}}')
        """,
        use_legacy_sql=False,
        location=BQ_LOCATION,
    )

    # --------------------------------------------------------------------------
    # 3. Task Dependency Chain
    # --------------------------------------------------------------------------
    # Both upstream sensors (`wait_for_manifest` and `wait_for_source_table`) run in
    # parallel and must succeed before `query_meta`, SQL aggregation, and validation run.
    (
        [wait_for_manifest, wait_for_source_table]
        >> query_meta
        >> aggregate_daily_sales
        >> validate_output_not_empty
    )


# Instantiate the DAG so Airflow registers it
deferrable_sensors_and_taskflow_pipeline()
