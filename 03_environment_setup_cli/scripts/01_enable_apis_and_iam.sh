#!/usr/bin/env bash
# ==============================================================================
# Chapter 03 — Script 01: Enable Required GCP APIs & Configure IAM Roles
# Reference Notebook: ../03_environment_setup_cli.ipynb (Steps 1 & 2)
#
# What this script does:
#   1. Loads your project and Service Account variables from the root `.env` file.
#   2. Sets the active `gcloud` project (`GCP_PROJECT_ID`).
#   3. Enables all Google Cloud APIs required by Cloud Composer, Dataproc, and BigQuery.
#   4. Creates a custom Service Account (`SERVICE_ACCOUNT_NAME`) for Airflow workers.
#   5. Grants least-privilege IAM roles to the custom worker Service Account.
#   6. Grants `roles/composer.ServiceAgentV2Ext` to Google's managed Composer
#      Service Agent (`service-<PROJECT_NUMBER>@cloudcomposer-accounts.iam.gserviceaccount.com`).
# ==============================================================================
set -euo pipefail

# Locate the repository root and source `.env`
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
else
  echo "Error: .env file not found at ${REPO_ROOT}/.env. Run 'cp .env.example .env' first."
  exit 1
fi

# ------------------------------------------------------------------------------
# Step 1: Set Active Project & Enable Required APIs
# ------------------------------------------------------------------------------
echo "==> [1/4] Setting active GCP project to '${GCP_PROJECT_ID}'..."
gcloud config set project "${GCP_PROJECT_ID}"

echo "==> [2/4] Enabling required Google Cloud APIs..."
gcloud services enable \
  composer.googleapis.com \
  dataproc.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  compute.googleapis.com \
  container.googleapis.com \
  sqladmin.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  cloudbuild.googleapis.com \
  cloudfunctions.googleapis.com \
  --project="${GCP_PROJECT_ID}"

# ------------------------------------------------------------------------------
# Step 2: Create Custom Worker Service Account (Idempotent)
# ------------------------------------------------------------------------------
echo "==> [3/4] Creating custom Service Account '${SERVICE_ACCOUNT_NAME}' (if not exists)..."
if ! gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
    --display-name="Service Account for Cloud Composer Environment" \
    --project="${GCP_PROJECT_ID}"
else
  echo "    Service Account '${SERVICE_ACCOUNT_EMAIL}' already exists."
fi

# ------------------------------------------------------------------------------
# Step 3: Bind IAM Roles to Worker Service Account & Composer Service Agent
# ------------------------------------------------------------------------------
# - composer.worker & container.nodeServiceAccount: Core roles to run Composer worker pods
# - compute.networkUser: Required to attach workers and Dataproc batches to our custom VPC
# - storage.objectAdmin: Read/write DAGs, logs, and Spark staging files in GCS
# - bigquery.*: Create datasets/tables, run queries, and read via BigQuery Storage API
# - dataproc.* & iam.serviceAccountUser: Submit and run Dataproc Serverless Spark batches
echo "==> [4/4] Granting IAM roles to '${SERVICE_ACCOUNT_EMAIL}'..."
ROLES=(
  "roles/composer.worker"
  "roles/container.nodeServiceAccount"
  "roles/compute.networkUser"
  "roles/storage.objectAdmin"
  "roles/bigquery.dataEditor"
  "roles/bigquery.jobUser"
  "roles/bigquery.readSessionUser"
  "roles/dataproc.editor"
  "roles/dataproc.worker"
  "roles/iam.serviceAccountUser"
  "roles/logging.logWriter"
  "roles/monitoring.metricWriter"
)

for ROLE in "${ROLES[@]}"; do
  echo "    Binding ${ROLE}..."
  gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="${ROLE}" \
    --project="${GCP_PROJECT_ID}" \
    --quiet >/dev/null
done

# Retrieve the numeric Project Number to construct the Google-managed Composer Service Agent email
PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
COMPOSER_SERVICE_AGENT="service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com"

echo "    Granting roles/composer.ServiceAgentV2Ext to '${COMPOSER_SERVICE_AGENT}'..."
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SERVICE_AGENT}" \
  --role="roles/composer.ServiceAgentV2Ext" \
  --project="${GCP_PROJECT_ID}" \
  --quiet >/dev/null || true

echo "API activation and IAM configuration completed for '${GCP_PROJECT_ID}'."
