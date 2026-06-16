#!/usr/bin/env bash
# ==============================================================================
# Chapter 03 — Script 03: Provision Cloud Composer Environment
# Reference Notebook: ../03_environment_setup_cli.ipynb
#
# What this script does:
#   Provisions a Private-IP Cloud Composer 3 environment (`COMPOSER_ENV_NAME`)
#   using the custom VPC, Subnetwork, Service Account, and DAGs bucket defined
#   in your root `.env` file.
#
# Note: Environment creation typically takes 15–25 minutes to complete.
# ==============================================================================
set -euo pipefail

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

PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Determine whether the custom Service Account has project-level roles or if we
# should fall back to the pre-authorized Compute Engine default service account
EFFECTIVE_SA="${SERVICE_ACCOUNT_EMAIL}"
if ! gcloud projects get-iam-policy "${GCP_PROJECT_ID}" \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --format="value(bindings.role)" 2>/dev/null | grep -q .; then
  echo "    Note: '${SERVICE_ACCOUNT_EMAIL}' has no project-level IAM bindings in this sandbox."
  echo "    Using Compute Engine default service account: '${COMPUTE_SA}'"
  EFFECTIVE_SA="${COMPUTE_SA}"
fi

echo "==> Provisioning Cloud Composer environment '${COMPOSER_ENV_NAME}' in '${GCP_REGION}' (Service Account: ${EFFECTIVE_SA})..."
gcloud composer environments create "${COMPOSER_ENV_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --image-version="${COMPOSER_IMAGE_VERSION}" \
  --environment-size=SMALL \
  --enable-private-environment \
  --network="projects/${GCP_PROJECT_ID}/global/networks/${VPC_NETWORK_NAME}" \
  --subnetwork="projects/${GCP_PROJECT_ID}/regions/${GCP_REGION}/subnetworks/${SUBNET_NAME}" \
  --service-account="${EFFECTIVE_SA}" \
  --scheduler-cpu=1 \
  --scheduler-memory=4GB \
  --scheduler-count=1 \
  --worker-cpu=1 \
  --worker-memory=4GB \
  --worker-storage=1GB \
  --min-workers=1 \
  --max-workers=3 \
  --storage-bucket="${GCS_DAG_BUCKET}" \
  --labels="mode=training,user=eilinlux"

echo "Cloud Composer environment '${COMPOSER_ENV_NAME}' is ready!"
