"""
Chapter 04 Reference PySpark Job: BigQuery Sales Aggregation (bq_etl_job.py)
============================================================================
Reference Notebook: ../04_dataproc_serverless_etl.ipynb
Orchestrating DAG:  ../dags/bq_etl_dag.py
Chapter Guide:      ../README.md

Execution Flow:
  1. Parses CLI arguments (`--input-table`, `--output-table`, `--processing-date`,
     and `--gcs-temp-bucket`) passed by `DataprocCreateBatchOperator`.
  2. Creates a `SparkSession` configured with `temporaryGcsBucket` for the
     pre-installed Dataproc Serverless Spark BigQuery connector.
  3. Reads the upstream `sales_data` table from BigQuery via the BigQuery Storage Read API.
  4. Filters rows matching `--processing-date` and computes `SUM(amount) AS total_sales`
     grouped by `product_id`.
  5. Writes the aggregated DataFrame back to `--output-table` in BigQuery (`overwrite` mode).
"""

from __future__ import annotations

import argparse
import sys

# PySpark SQL imports for session management, built-in column functions, and data types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments supplied by the Airflow `DataprocCreateBatchOperator`.

    Example invocation by Dataproc Serverless:
      python bq_etl_job.py \
        --input-table=gai-community29oct-7500.example_composer_workshop.sales_data \
        --output-table=gai-community29oct-7500.example_composer_workshop.filtered_sales \
        --processing-date=2025-06-12 \
        --gcs-temp-bucket=gai-community29oct-7500-dataproc-temp
    """
    parser = argparse.ArgumentParser(
        description="Filter and aggregate BigQuery sales data using PySpark on Dataproc Serverless."
    )
    parser.add_argument(
        "--input-table",
        type=str,
        required=True,
        help="Source BigQuery table in 'project.dataset.table' format.",
    )
    parser.add_argument(
        "--output-table",
        type=str,
        required=True,
        help="Destination BigQuery table in 'project.dataset.table' format.",
    )
    parser.add_argument(
        "--processing-date",
        type=str,
        required=True,
        help="Target transaction date to process in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--gcs-temp-bucket",
        type=str,
        required=True,
        help="Cloud Storage bucket used by the Spark BigQuery connector for temporary staging.",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the PySpark BigQuery ETL batch job."""
    # Parse CLI arguments passed by the Airflow DAG
    args = parse_args()

    # --------------------------------------------------------------------------
    # 1. Initialize SparkSession
    # --------------------------------------------------------------------------
    # Note: Dataproc Serverless Runtime 1.1+ / 2.2+ pre-installs the Google Cloud
    # Spark BigQuery connector (`spark-bigquery-with-dependencies`).
    # Do NOT hardcode `spark.jars.packages` here, as downloading jars from Maven
    # Central at runtime will fail inside private-IP VPC subnets without Cloud NAT.
    spark = (
        SparkSession.builder.appName("SalesDataProcessor-BQ")
        .config("temporaryGcsBucket", args.gcs_temp_bucket)
        .getOrCreate()
    )

    try:
        # ----------------------------------------------------------------------
        # 2. Extract: Read Source Table from BigQuery
        # ----------------------------------------------------------------------
        print(f"Reading source data from BigQuery table: {args.input_table}")
        df = (
            spark.read.format("bigquery")
            .option("table", args.input_table)
            .load()
        )

        # Log the schema discovered from BigQuery metadata
        print("Source table schema:")
        df.printSchema()

        # ----------------------------------------------------------------------
        # 3. Transform: Filter by Date & Aggregate Total Sales per Product
        # ----------------------------------------------------------------------
        # The filter on `transaction_date` is automatically pushed down to BigQuery's
        # Storage Read API, scanning only the target date partition!
        print(f"Filtering and aggregating transactions for date: {args.processing_date}")
        processed_df = (
            df.filter(
                F.col("transaction_date") == F.lit(args.processing_date).cast(DateType())
            )
            .groupBy("product_id")
            .agg(F.sum("amount").alias("total_sales"))
            .withColumn("processing_date", F.lit(args.processing_date).cast(DateType()))
        )

        # Log transformed schema and row count for observability in Cloud Logging
        print("Transformed DataFrame schema:")
        processed_df.printSchema()
        row_count = processed_df.count()
        print(f"Number of aggregated product rows for {args.processing_date}: {row_count}")

        # ----------------------------------------------------------------------
        # 4. Load: Write Aggregated Results to Destination BigQuery Table
        # ----------------------------------------------------------------------
        # The Spark BigQuery connector stages intermediate Parquet/Avro files in
        # `temporaryGcsBucket` before triggering an atomic BigQuery load job.
        print(f"Writing aggregated results to BigQuery table: {args.output_table}")
        (
            processed_df.write.format("bigquery")
            .option("table", args.output_table)
            .option("temporaryGcsBucket", args.gcs_temp_bucket)
            .mode("overwrite")
            .save()
        )
        print("PySpark BigQuery ETL job completed successfully!")

    except Exception as exc:
        # Log the error to stderr so it surfaces clearly in Cloud Logging and Airflow task logs
        print(f"Error during PySpark execution: {exc}", file=sys.stderr)
        # Re-raise the exception so Dataproc Serverless marks the batch state as FAILED
        raise

    finally:
        # ----------------------------------------------------------------------
        # 5. Cleanup: Stop the SparkSession
        # ----------------------------------------------------------------------
        # Always close the SparkSession inside `finally` (and nowhere else) so
        # resources are released exactly once whether the job succeeds or fails.
        if "spark" in locals() and spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()