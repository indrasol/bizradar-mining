#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="$REPO_ROOT/backend/.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

# Creates storage account + blob container for CSV pointers.
# Required env:
#   AZ_RESOURCE_GROUP_BIZ
#   AZ_LOCATION_BIZ
#   AZ_STORAGE_ACCOUNT_BIZ
# Optional env:
#   AZ_SUBSCRIPTION_ID_BIZ
#   AZ_STORAGE_CONTAINER_BIZ (default: csv-ingest)

if [[ -n "${AZ_SUBSCRIPTION_ID_BIZ:-}" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION_ID_BIZ"
fi

: "${AZ_RESOURCE_GROUP_BIZ:?AZ_RESOURCE_GROUP_BIZ is required}"
: "${AZ_LOCATION_BIZ:?AZ_LOCATION_BIZ is required}"
: "${AZ_STORAGE_ACCOUNT_BIZ:?AZ_STORAGE_ACCOUNT_BIZ is required}"

AZ_STORAGE_CONTAINER_BIZ="${AZ_STORAGE_CONTAINER_BIZ:-csv-ingest}"

echo "Ensuring resource group exists: $AZ_RESOURCE_GROUP_BIZ"
az group create --name "$AZ_RESOURCE_GROUP_BIZ" --location "$AZ_LOCATION_BIZ" 1>/dev/null

echo "Ensuring storage account exists: $AZ_STORAGE_ACCOUNT_BIZ"
az storage account create \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --name "$AZ_STORAGE_ACCOUNT_BIZ" \
  --location "$AZ_LOCATION_BIZ" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --allow-blob-public-access false \
  --https-only true \
  --min-tls-version TLS1_2 \
  --allow-shared-key-access false 1>/dev/null

echo "Ensuring blob container exists: $AZ_STORAGE_CONTAINER_BIZ"
az storage container create \
  --name "$AZ_STORAGE_CONTAINER_BIZ" \
  --account-name "$AZ_STORAGE_ACCOUNT_BIZ" \
  --auth-mode login 1>/dev/null

echo "Storage provisioning complete."
