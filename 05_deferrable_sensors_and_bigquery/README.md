# Chapter 05 — Deferrable Sensors, TaskFlow API & BigQuery Quality Checks

## Overview

In Chapter 04, we used a placeholder `BashOperator` to simulate checking whether upstream data had arrived before running our ETL job. In production pipelines on **Google Cloud Composer**, you should use **Deferrable Sensors** and **BigQuery Data Quality Operators**.

### What We Are Going to Do
1. **Wait for Upstream Data with Deferrable Sensors (`deferrable=True`)**:
   * Use `GCSObjectExistenceSensor` to wait for a daily `_SUCCESS` manifest file in Cloud Storage (`gs://<raw-landing-bucket>/incoming/sales/{{ ds }}/_SUCCESS`).
   * Use `BigQueryTableExistenceSensor` to verify the upstream `sales_data` table exists in BigQuery.
   * Because both sensors run with `deferrable=True`, Airflow immediately releases the worker slot and delegates waiting to the lightweight asynchronous **Airflow Triggerer**.
2. **Prepare Dynamic Metadata with `@task` (TaskFlow API)**:
   * Build target table and date metadata cleanly in Python.
3. **Execute Pushdown SQL & Data Quality Assertions in BigQuery**:
   * Run a partitioned and clustered aggregation directly inside BigQuery using `BigQueryInsertJobOperator`.
   * Verify that the output partition contains at least one row using `BigQueryCheckOperator`.

---

## Chapter Contents

| File | Description |
| :--- | :--- |
| [`05_deferrable_sensors_and_bigquery.ipynb`](./05_deferrable_sensors_and_bigquery.ipynb) | Interactive notebook explaining standard vs. deferrable sensors, SQL pushdown, and data quality operators. |
| [`dags/sensor_bq_etl_dag.py`](./dags/sensor_bq_etl_dag.py) | Heavily commented production DAG combining deferrable GCS/BigQuery sensors, `@task`, `BigQueryInsertJobOperator`, and `BigQueryCheckOperator`. |
| [`scripts/01_deploy_sensor_dag.sh`](./scripts/01_deploy_sensor_dag.sh) | Uploads a sample `_SUCCESS` manifest file to GCS, deploys `sensor_bq_etl_dag.py` to Composer, and triggers the DAG. |

---

## How to Run the Script

From the repository root (after configuring [`.env`](../.env)):

```bash
bash 05_deferrable_sensors_and_bigquery/scripts/01_deploy_sensor_dag.sh
```
