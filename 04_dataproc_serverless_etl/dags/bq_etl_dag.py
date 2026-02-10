"""
Chapter 04 Reference DAG: Orchestrating Dataproc Serverless & BigQuery ETL
==========================================================================
Reference Notebook: ../04_dataproc_serverless_etl.ipynb
Companion PySpark:  ../spark_apps/bq_etl_job.py
Chapter Guide:      ../README.md

Pipeline Architecture:
  1. `check_bq_input_data_readiness` (BashOperator):
     Logs the target BigQuery table and logical processing date being executed.
  2. `create_or_check_bq_output_dataset` (BigQueryCreateEmptyDatasetOperator):
     Idempotently ensures the target BigQuery dataset exists (`if_exists="ignore"`).
  3. `create_or_check_bq_output_table` (BigQueryCreateTableOperator):
     Idempotently ensures the target BigQuery output table exists with the
     expected schema (`BQ_OUTPUT_TABLE_SCHEMA`).
  4. `submit_bq_to_bq_etl_spark_job` (DataprocCreateBatchOperator):
     Submits `bq_etl_job.py` to Dataproc Serverless in deferrable mode
     (`deferrable=True`), using dynamic runtime Jinja macros (`{{ ds_nodash }}`,
     `{{ ts_nodash | lower }}`) rather than top-level `datetime.now()`.
"""

from __future__ import annotations

import os
import pendulum

# ------------------------------------------------------------------------------
# 0. Local `.env` Loading (for local testing with `uv run pytest`)
# ------------------------------------------------------------------------------
# When testing locally, `python-dotenv` loads variables from the root `.env` file.
# Inside Cloud Composer, `python-dotenv` is optional and variables are supplied
# via environment variables or Airflow Variables (`var.value.*`).
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
)
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# ------------------------------------------------------------------------------
# 1. Environment & Project Configuration Variables
# ------------------------------------------------------------------------------
# Google Cloud Project ID and Region where Cloud Composer and Dataproc Serverless run
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "gai-community29oct-7500")
GCP_REGION = os.getenv("GCP_REGION", "europe-west1")

# Cloud Storage bucket associated with your Composer environment (stores DAGs & Spark code)
GCS_DAG_BUCKET = os.getenv("GCS_DAG_BUCKET", "gai-community29oct-7500-composer-dags")

# Custom VPC subnetwork name used by Dataproc Serverless worker VMs.
# Evaluates Airflow Variable `SUBNET_NAME` at task execution time (not DAG parse time).
SUBNET_NAME_VAR = (
    "{{ var.value.get('SUBNET_NAME', '"
    + os.getenv("SUBNET_NAME", "composer-workshop-subnet")
    + "') }}"
)

# Full GCS URI pointing to the PySpark script uploaded by `02_deploy_and_trigger_dag.sh`
PYSPARK_APP_URI = f"gs://{GCS_DAG_BUCKET}/spark_apps/bq_etl_job.py"

# BigQuery Dataset, Input Table, and Output Table ("project.dataset.table")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "example_composer_workshop")
BQ_INPUT_TABLE_ID = os.getenv(
    "BQ_INPUT_TABLE_ID", f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.sales_data"
)
BQ_OUTPUT_TABLE_ID = os.getenv(
    "BQ_OUTPUT_TABLE_ID", f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.filtered_sales"
)

# Dynamic Processing Date:
# - When triggered manually with JSON config `{"processing_date": "2025-06-12"}`,
#   it uses `dag_run.conf['processing_date']`.
# - When triggered on a daily schedule without config, it falls back to `ds` (YYYY-MM-DD).
PROCESSING_DATE = "{{ dag_run.conf.get('processing_date', ds) }}"

# Schema definition for the destination BigQuery table (`filtered_sales`).
# Must match the DataFrame columns produced by `bq_etl_job.py`.
BQ_OUTPUT_TABLE_SCHEMA = [
    {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "total_sales", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"},
]

# Dedicated GCS bucket used by Dataproc Serverless for job staging and by the
# Spark BigQuery connector for temporary Avro/Parquet write buffers.
SPARK_TEMP_BUCKET = os.getenv("SPARK_TEMP_BUCKET", "gai-community29oct-7500-dataproc-temp")

# Custom Worker Service Account email fetched at runtime from Airflow Variables.
# If this Variable is not set in Airflow UI / CLI, Airflow raises a clear KeyError
# during task rendering (explained in `04_dataproc_serverless_etl.ipynb`).
SERVICE_ACCOUNT_EMAIL_VAR = "{{ var.value.airflow_worker_gcp_service_account }}"


# ------------------------------------------------------------------------------
# 2. DAG Definition (`@dag`)
# ------------------------------------------------------------------------------
@dag(
    dag_id="dataproc_serverless_bq_to_bq_etl_create_batch",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["chapter-04", "etl", "bigquery", "spark", "dataproc", "serverless"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "project_id": GCP_PROJECT_ID,
    },
)
def bq_to_bq_etl_create_batch_pipeline():
    """Orchestrates BigQuery target setup and Dataproc Serverless PySpark execution."""

    # --------------------------------------------------------------------------
    # Task 1: Readiness Check (Simulated via BashOperator)
    # --------------------------------------------------------------------------
    # Prints the upstream BigQuery table and target processing date.
    # (In Chapter 05, we replace this with real Deferrable GCS & BigQuery Sensors!)
    check_bq_input_data = BashOperator(
        task_id="check_bq_input_data_readiness",
        bash_command=(
            f"echo 'Checking if BigQuery table {BQ_INPUT_TABLE_ID} is ready "
            f"for processing on date {PROCESSING_DATE}...'"
        ),
    )

    # Split "project.dataset.table" into dataset_id and table_id for the BigQuery operators
    _output_dataset_id_only = BQ_OUTPUT_TABLE_ID.split(".")[1]
    _output_table_id = BQ_OUTPUT_TABLE_ID.split(".")[2]

    # --------------------------------------------------------------------------
    # Task 2: Ensure BigQuery Output Dataset Exists
    # --------------------------------------------------------------------------
    # `if_exists="ignore"` prevents the task from failing when the dataset already exists.
    create_or_check_bq_output_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_or_check_bq_output_dataset",
        dataset_id=_output_dataset_id_only,
        project_id=GCP_PROJECT_ID,
        if_exists="ignore",
        gcp_conn_id="google_cloud_default",
    )

    # --------------------------------------------------------------------------
    # Task 3: Ensure BigQuery Output Table Exists
    # --------------------------------------------------------------------------
    # Uses `BigQueryCreateTableOperator` (which replaced the deprecated
    # `BigQueryCreateEmptyTableOperator` in apache-airflow-providers-google 10.x+).
    create_or_check_bq_output_table = BigQueryCreateTableOperator(
        task_id="create_or_check_bq_output_table",
        project_id=GCP_PROJECT_ID,
        dataset_id=_output_dataset_id_only,
        table_id=_output_table_id,
        table_resource={
            "schema": {"fields": BQ_OUTPUT_TABLE_SCHEMA},
        },
        if_exists="ignore",
        gcp_conn_id="google_cloud_default",
    )

    # --------------------------------------------------------------------------
    # Task 4: Submit PySpark Job to Dataproc Serverless (`DataprocCreateBatchOperator`)
    # --------------------------------------------------------------------------
    # IMPORTANT BEST PRACTICE:
    # Never use `datetime.datetime.now()` at the top level of a DAG file to generate
    # `batch_id`, because Airflow re-parses the DAG file every ~30 seconds.
    # Instead, use runtime Jinja macros (`{{ ds_nodash }}` and `{{ ts_nodash | lower }}`).
    submit_bq_to_bq_etl_batch = DataprocCreateBatchOperator(
        task_id="submit_bq_to_bq_etl_spark_job",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        # Must match regex `^[a-z0-9][a-z0-9-]{2,61}[a-z0-9]$`
        batch_id="etl-batch-{{ ds_nodash }}-{{ ts_nodash | lower }}",
        batch={
            # 4a. PySpark script URI and command-line arguments passed to `bq_etl_job.py`
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_APP_URI,
                "args": [
                    f"--input-table={BQ_INPUT_TABLE_ID}",
                    f"--output-table={BQ_OUTPUT_TABLE_ID}",
                    f"--processing-date={PROCESSING_DATE}",
                    f"--gcs-temp-bucket={SPARK_TEMP_BUCKET}",
                ],
            },
            # 4b. Dataproc Serverless runtime version (1.1 includes Spark 3.4 & BQ connector)
            "runtime_config": {
                "version": "1.1",
            },
            # 4c. Execution environment: custom Service Account, custom Subnet, and Staging Bucket
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT_EMAIL_VAR,
                    "subnetwork_uri": (
                        f"projects/{GCP_PROJECT_ID}/regions/{GCP_REGION}/"
                        f"subnetworks/{SUBNET_NAME_VAR}"
                    ),
                    "staging_bucket": SPARK_TEMP_BUCKET,
                },
            },
            # 4d. GCP resource labels for cost tracking and governance
            "labels": {
                "airflow_dag_id": "{{ dag.dag_id }}",
                "mode": "training",
                "user": "zelda",
            },
        },
        # `deferrable=True` releases the Airflow worker slot while the Spark batch runs
        # on Dataproc Serverless, polling status asynchronously via the Airflow Triggerer.
        deferrable=True,
    )

    # --------------------------------------------------------------------------
    # 3. Task Dependency Chain
    # --------------------------------------------------------------------------
    (
        check_bq_input_data
        >> create_or_check_bq_output_dataset
        >> create_or_check_bq_output_table
        >> submit_bq_to_bq_etl_batch
    )


# Instantiate the DAG so Airflow discovers and schedules it
bq_to_bq_etl_create_batch_pipeline()