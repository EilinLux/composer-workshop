#!/usr/bin/env bash
# ==============================================================================
# Chapter 03 — Script 04: Cloud Composer Environment Teardown & Cleanup
# Reference Notebook: ../03_environment_setup_cli.ipynb (Environment Teardown)
#
# What this script does:
#   Deletes the Cloud Composer environment (`COMPOSER_ENV_NAME`) when you finish
#   the workshop so your GCP project does not incur ongoing hourly charges.
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
  echo "Error: .env file not found at ${REPO_ROOT}/.env."
  exit 1
fi

echo "==> Deleting Cloud Composer environment '${COMPOSER_ENV_NAME}' in '${GCP_REGION}'..."
gcloud composer environments delete "${COMPOSER_ENV_NAME}" \
  --location="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --quiet

echo "Cloud Composer environment '${COMPOSER_ENV_NAME}' deleted."
