#!/usr/bin/env bash
# ==============================================================================
# Chapter 05 — Script 01: Create Sample Manifest & Deploy Deferrable Sensor DAG
# Reference Notebook: ../05_deferrable_sensors_and_bigquery.ipynb
# Reference Code:     ../dags/sensor_bq_etl_dag.py
#
# What this script does:
#   1. Creates an empty `_SUCCESS` marker file in your landing bucket
#      (`gs://${RAW_LANDING_BUCKET}/incoming/sales/${DEFAULT_PROCESSING_DATE}/_SUCCESS`)
#      so `GCSObjectExistenceSensor` succeeds when triggered.
#   2. Uploads `sensor_bq_etl_dag.py` to `gs://${GCS_DAG_BUCKET}/dags/`.
#   3. Sets the `gcp_project_id` and `raw_landing_bucket` Airflow Variables in Composer.
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

echo "==> [1/3] Uploading sample _SUCCESS marker to 'gs://${RAW_LANDING_BUCKET}/incoming/sales/${DEFAULT_PROCESSING_DATE}/_SUCCESS'..."
echo "OK" | gcloud storage cp - \
  "gs://${RAW_LANDING_BUCKET}/incoming/sales/${DEFAULT_PROCESSING_DATE}/_SUCCESS" \
  --project="${GCP_PROJECT_ID}"

echo "==> [2/3] Uploading 'sensor_bq_etl_dag.py' to 'gs://${GCS_DAG_BUCKET}/dags/'..."
gcloud storage cp \
  "${CHAPTER_DIR}/dags/sensor_bq_etl_dag.py" \
  "gs://${GCS_DAG_BUCKET}/dags/sensor_bq_etl_dag.py" \
  --project="${GCP_PROJECT_ID}"

echo "==> [3/3] Setting Airflow Variables in Composer environment '${COMPOSER_ENV_NAME}'..."
if gcloud composer environments describe "${COMPOSER_ENV_NAME}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    variables set -- gcp_project_id "${GCP_PROJECT_ID}"

  gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    variables set -- raw_landing_bucket "${RAW_LANDING_BUCKET}"
else
  echo "    Note: Composer environment '${COMPOSER_ENV_NAME}' is not running yet; skipped Airflow CLI variable setup."
fi

echo "Chapter 05 Sensor DAG deployed!"
