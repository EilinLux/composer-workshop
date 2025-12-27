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

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
else
  echo "Error: .env file not found at ${REPO_ROOT}/.env. Run 'cp .env.example .env' first."
  exit 1
fi

echo "==> Provisioning Cloud Composer environment '${COMPOSER_ENV_NAME}' in '${GCP_REGION}'..."
gcloud composer environments create "${COMPOSER_ENV_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --image-version="${COMPOSER_IMAGE_VERSION}" \
  --environment-size=SMALL \
  --enable-private-environment \
  --network="projects/${GCP_PROJECT_ID}/global/networks/${VPC_NETWORK_NAME}" \
  --subnetwork="projects/${GCP_PROJECT_ID}/regions/${GCP_REGION}/subnetworks/${SUBNET_NAME}" \
  --service-account="${SERVICE_ACCOUNT_EMAIL}" \
  --scheduler-cpu=1 \
  --scheduler-memory=4GB \
  --scheduler-count=1 \
  --worker-cpu=1 \
  --worker-memory=4GB \
  --worker-storage=1GB \
  --min-workers=1 \
  --max-workers=3 \
  --web-server-allow-all \
  --storage-bucket="${GCS_DAG_BUCKET}" \
  --labels="mode=training,user=zelda"

echo "Cloud Composer environment '${COMPOSER_ENV_NAME}' is ready!"
