#!/usr/bin/env bash
# ==============================================================================
# Chapter 04 — Script 01: Seed Upstream BigQuery Dataset & Table
# Reference Notebook: ../04_dataproc_serverless_etl.ipynb
# Reference SQL:      ../sql/seed_sales_data.sql
#
# What this script does:
#   1. Loads `GCP_PROJECT_ID`, `BQ_LOCATION`, and `BQ_DATASET_ID` from `.env`.
#   2. Executes `../sql/seed_sales_data.sql` using the `bq query` CLI.
#   3. Runs a verification `SELECT` query so you can immediately see the seeded rows.
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
fi

PROJECT_ID="${1:-${GCP_PROJECT_ID:-gai-community29oct-7500}}"
LOCATION="${2:-${BQ_LOCATION:-EU}}"
DATASET_ID="${BQ_DATASET_ID:-example_composer_workshop}"

echo "==> [1/2] Seeding BigQuery dataset '${DATASET_ID}' and table 'sales_data' in project '${PROJECT_ID}' (${LOCATION})..."
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false < "${CHAPTER_DIR}/sql/seed_sales_data.sql"

echo "==> [2/2] Verifying seeded rows in '${PROJECT_ID}.${DATASET_ID}.sales_data'..."
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.sales_data\` ORDER BY transaction_id"
