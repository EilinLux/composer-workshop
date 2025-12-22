#!/usr/bin/env bash
# ==============================================================================
# Chapter 03 — Script 02: Provision Custom VPC, Subnetwork, Firewall & Buckets
# Reference Notebook: ../03_environment_setup_cli.ipynb (Step 3)
#
# What this script does:
#   1. Creates a custom-mode VPC network (`VPC_NETWORK_NAME`) so we control all CIDR blocks.
#   2. Creates a regional subnetwork (`SUBNET_NAME`) with:
#      - `--enable-private-ip-google-access`: Allows private-IP nodes/batches to reach
#        Google APIs (BigQuery, GCS, Composer control plane) without public IPs.
#      - Secondary IP ranges (`GKE_PODS_RANGE_NAME`, `GKE_SERVICES_RANGE_NAME`) for
#        VPC-native GKE / Composer 2 compatibility.
#   3. Creates an internal firewall rule (`allow-internal-dataproc-traffic`) allowing
#      Spark driver and executor nodes on Dataproc Serverless to communicate across
#      all TCP/UDP/ICMP ports within the subnet's primary CIDR range.
#   4. Creates the Cloud Storage buckets (`GCS_DAG_BUCKET` and `SPARK_TEMP_BUCKET`).
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

# ------------------------------------------------------------------------------
# Step 1: Create Custom-Mode VPC Network
# ------------------------------------------------------------------------------
echo "==> [1/4] Creating custom-mode VPC network '${VPC_NETWORK_NAME}'..."
if ! gcloud compute networks describe "${VPC_NETWORK_NAME}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute networks create "${VPC_NETWORK_NAME}" \
    --subnet-mode=custom \
    --project="${GCP_PROJECT_ID}"
else
  echo "    VPC '${VPC_NETWORK_NAME}' already exists."
fi

# ------------------------------------------------------------------------------
# Step 2: Create Subnetwork with Private Google Access & Secondary IP Ranges
# ------------------------------------------------------------------------------
echo "==> [2/4] Creating subnetwork '${SUBNET_NAME}' in '${GCP_REGION}'..."
if ! gcloud compute networks subnets describe "${SUBNET_NAME}" --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute networks subnets create "${SUBNET_NAME}" \
    --network="${VPC_NETWORK_NAME}" \
    --range="${PRIMARY_IP_RANGE}" \
    --region="${GCP_REGION}" \
    --enable-private-ip-google-access \
    --secondary-range="${GKE_PODS_RANGE_NAME}=${GKE_PODS_IP_RANGE}" \
    --secondary-range="${GKE_SERVICES_RANGE_NAME}=${GKE_SERVICES_IP_RANGE}" \
    --project="${GCP_PROJECT_ID}"
else
  echo "    Subnetwork '${SUBNET_NAME}' already exists."
fi

# ------------------------------------------------------------------------------
# Step 3: Create Internal Firewall Rule for Spark Executor Communication
# ------------------------------------------------------------------------------
# Dataproc Serverless batches require open internal communication (TCP/UDP/ICMP)
# between driver and executor VMs within the primary subnet CIDR range.
echo "==> [3/4] Creating internal firewall rule 'allow-internal-dataproc-traffic'..."
if ! gcloud compute firewall-rules describe allow-internal-dataproc-traffic --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute firewall-rules create allow-internal-dataproc-traffic \
    --project="${GCP_PROJECT_ID}" \
    --network="${VPC_NETWORK_NAME}" \
    --action=ALLOW \
    --rules=tcp:1-65535,udp:1-65535,icmp \
    --source-ranges="${PRIMARY_IP_RANGE}" \
    --destination-ranges="${PRIMARY_IP_RANGE}" \
    --direction=INGRESS \
    --priority=1000 \
    --description="Allow internal TCP/UDP/ICMP traffic within the Composer/Dataproc subnet."
else
  echo "    Firewall rule 'allow-internal-dataproc-traffic' already exists."
fi

# ------------------------------------------------------------------------------
# Step 4: Create Cloud Storage Buckets for DAGs and Spark Staging
# ------------------------------------------------------------------------------
echo "==> [4/4] Creating Cloud Storage buckets ('gs://${GCS_DAG_BUCKET}' and 'gs://${SPARK_TEMP_BUCKET}')..."
for BUCKET in "${GCS_DAG_BUCKET}" "${SPARK_TEMP_BUCKET}"; do
  if ! gcloud storage buckets describe "gs://${BUCKET}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud storage buckets create "gs://${BUCKET}" \
      --location="${GCP_REGION}" \
      --uniform-bucket-level-access \
      --project="${GCP_PROJECT_ID}"
  else
    echo "    Bucket 'gs://${BUCKET}' already exists."
  fi
done

echo "Networking and Cloud Storage setup completed successfully!"
