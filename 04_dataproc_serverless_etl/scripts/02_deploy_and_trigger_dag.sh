#!/usr/bin/env bash
# ==============================================================================
# Chapter 04 — Script 02: Deploy PySpark Job & DAG to Composer and Trigger Run
# Reference Notebook: ../04_dataproc_serverless_etl.ipynb (Steps 1–4)
# Reference Code:     ../dags/bq_etl_dag.py, ../spark_apps/bq_etl_job.py
#
# What this script does:
#   1. Uploads `bq_etl_job.py` to `gs://${GCS_DAG_BUCKET}/spark_apps/`.
#   2. Uploads `bq_etl_dag.py` to `gs://${GCS_DAG_BUCKET}/dags/`.
#   3. Configures the required Airflow Variables (`airflow_worker_gcp_service_account`
#      and `SUBNET_NAME`) inside your Cloud Composer environment.
#   4. Triggers a manual DAG run for `--processing-date` (`DEFAULT_PROCESSING_DATE`).
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${CHAPTER_DIR}/.." && pwd)"

if command -v uv >/dev/null 2>&1; then
  UV_PY312="$(uv python find 3.12 2>/dev/null || true)"
  if [[ -n "${UV_PY312}" ]]; then
    export CLOUDSDK_PYTHON="${UV_PY312}"
  fi
fi

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
else
  echo "Error: .env file not found at ${REPO_ROOT}/.env. Run 'cp .env.example .env' first."
  exit 1
fi

PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
EFFECTIVE_SA="${SERVICE_ACCOUNT_EMAIL}"
if ! gcloud projects get-iam-policy "${GCP_PROJECT_ID}" \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --format="value(bindings.role)" 2>/dev/null | grep -q .; then
  EFFECTIVE_SA="${COMPUTE_SA}"
fi

echo "==> [1/4] Uploading PySpark job to 'gs://${GCS_DAG_BUCKET}/spark_apps/bq_etl_job.py'..."
gcloud storage cp \
  "${CHAPTER_DIR}/spark_apps/bq_etl_job.py" \
  "gs://${GCS_DAG_BUCKET}/spark_apps/bq_etl_job.py" \
  --project="${GCP_PROJECT_ID}"

echo "==> [2/4] Uploading Airflow DAG to 'gs://${GCS_DAG_BUCKET}/dags/bq_etl_dag.py'..."
gcloud storage cp \
  "${CHAPTER_DIR}/dags/bq_etl_dag.py" \
  "gs://${GCS_DAG_BUCKET}/dags/bq_etl_dag.py" \
  --project="${GCP_PROJECT_ID}"

echo "==> [3/4] Setting Airflow Variables in Composer environment '${COMPOSER_ENV_NAME}'..."
if gcloud composer environments describe "${COMPOSER_ENV_NAME}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    variables set -- airflow_worker_gcp_service_account "${EFFECTIVE_SA}"

  gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    variables set -- SUBNET_NAME "${SUBNET_NAME}"

  echo "==> [4/4] Triggering DAG 'dataproc_serverless_bq_to_bq_etl_create_batch' for date '${DEFAULT_PROCESSING_DATE}'..."
  gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    dags trigger -- dataproc_serverless_bq_to_bq_etl_create_batch \
    --conf="{\"processing_date\": \"${DEFAULT_PROCESSING_DATE}\"}"
  echo "DAG triggered! Monitor progress in the Airflow Web UI."
else
  echo "    Note: Composer environment '${COMPOSER_ENV_NAME}' is not running yet; DAG and PySpark files are staged in gs://${GCS_DAG_BUCKET}."
fi
