#!/usr/bin/env bash
# ==============================================================================
# Chapter 03 — Script 01: Enable Required GCP APIs & Configure IAM Roles
# Reference Notebook: ../03_environment_setup_cli.ipynb (Steps 1 & 2)
#
# What this script does:
#   1. Loads your project and Service Account variables from the root `.env` file.
#   2. Sets the active `gcloud` project (`GCP_PROJECT_ID`) and ADC quota project.
#   3. Enables all Google Cloud APIs required by Cloud Composer, Dataproc, and BigQuery.
#   4. Creates a custom Service Account (`SERVICE_ACCOUNT_NAME`) for Airflow workers.
#   5. Grants least-privilege IAM roles to the custom worker Service Account (or
#      gracefully falls back to the pre-authorized Compute Engine default service
#      account `<PROJECT_NUMBER>-compute@developer.gserviceaccount.com` in
#      sandbox/lab environments where project-level `setIamPolicy` is restricted).
# ==============================================================================
set -euo pipefail

# Locate the repository root and source `.env`
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

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

# ------------------------------------------------------------------------------
# Step 1: Set Active Project, Quota Project & Enable Required APIs
# ------------------------------------------------------------------------------
echo "==> [1/4] Setting active GCP project and ADC quota project to '${GCP_PROJECT_ID}'..."
gcloud config set project "${GCP_PROJECT_ID}"
gcloud config set billing/quota_project "${GCP_PROJECT_ID}" >/dev/null 2>&1 || true

echo "==> [2/4] Enabling required Google Cloud APIs..."
gcloud services enable \
  cloudresourcemanager.googleapis.com \
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

gcloud auth application-default set-quota-project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1 || true

# ------------------------------------------------------------------------------
# Step 2: Create Custom Worker Service Account (Idempotent)
# ------------------------------------------------------------------------------
echo "==> [3/4] Creating custom Service Account '${SERVICE_ACCOUNT_NAME}' (if not exists)..."
if ! gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
    --display-name="Service Account for Cloud Composer Environment" \
    --project="${GCP_PROJECT_ID}" || true
else
  echo "    Service Account '${SERVICE_ACCOUNT_EMAIL}' already exists."
fi

# ------------------------------------------------------------------------------
# Step 3: Bind IAM Roles to Worker Service Account & Composer Service Agent
# ------------------------------------------------------------------------------
PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
COMPOSER_SERVICE_AGENT="service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com"

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

if gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/composer.worker" \
  --project="${GCP_PROJECT_ID}" \
  --quiet >/dev/null 2>&1; then
  for ROLE in "${ROLES[@]}"; do
    echo "    Binding ${ROLE}..."
    gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
      --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
      --role="${ROLE}" \
      --project="${GCP_PROJECT_ID}" \
      --quiet >/dev/null
  done

  echo "    Granting roles/composer.ServiceAgentV2Ext to '${COMPOSER_SERVICE_AGENT}'..."
  gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${COMPOSER_SERVICE_AGENT}" \
    --role="roles/composer.ServiceAgentV2Ext" \
    --project="${GCP_PROJECT_ID}" \
    --quiet >/dev/null 2>&1 || true
else
  echo "    Note: Project-level setIamPolicy is restricted for this account in the lab environment."
  echo "    Falling back to the pre-authorized Compute Engine default service account:"
  echo "      ${COMPUTE_SA}"
  echo "    (Bucket-level Storage Admin permissions will be bound in 02_setup_vpc_and_buckets.sh)"
fi

echo "API activation and IAM configuration completed for '${GCP_PROJECT_ID}'."
