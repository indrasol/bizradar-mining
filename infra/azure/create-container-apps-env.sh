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

# Creates Log Analytics workspace + Container Apps environment.
# Required env:
#   AZ_RESOURCE_GROUP_BIZ
#   AZ_LOCATION_BIZ
#   AZ_CONTAINERAPPS_ENV_BIZ
#   AZ_LOG_ANALYTICS_WS_BIZ
# Optional env:
#   AZ_SUBSCRIPTION_ID_BIZ

if [[ -n "${AZ_SUBSCRIPTION_ID_BIZ:-}" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION_ID_BIZ"
fi

: "${AZ_RESOURCE_GROUP_BIZ:?AZ_RESOURCE_GROUP_BIZ is required}"
: "${AZ_LOCATION_BIZ:?AZ_LOCATION_BIZ is required}"
: "${AZ_CONTAINERAPPS_ENV_BIZ:?AZ_CONTAINERAPPS_ENV_BIZ is required}"
: "${AZ_LOG_ANALYTICS_WS_BIZ:?AZ_LOG_ANALYTICS_WS_BIZ is required}"

echo "Ensuring resource group exists: $AZ_RESOURCE_GROUP_BIZ"
az group create --name "$AZ_RESOURCE_GROUP_BIZ" --location "$AZ_LOCATION_BIZ" 1>/dev/null

echo "Ensuring Log Analytics workspace exists: $AZ_LOG_ANALYTICS_WS_BIZ"
az monitor log-analytics workspace create \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --workspace-name "$AZ_LOG_ANALYTICS_WS_BIZ" \
  --location "$AZ_LOCATION_BIZ" 1>/dev/null

WS_CUSTOMER_ID="$(az monitor log-analytics workspace show \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --workspace-name "$AZ_LOG_ANALYTICS_WS_BIZ" \
  --query customerId -o tsv)"

WS_SHARED_KEY="$(az monitor log-analytics workspace get-shared-keys \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --workspace-name "$AZ_LOG_ANALYTICS_WS_BIZ" \
  --query primarySharedKey -o tsv)"

echo "Ensuring Container Apps environment exists: $AZ_CONTAINERAPPS_ENV_BIZ"
az containerapp env create \
  --name "$AZ_CONTAINERAPPS_ENV_BIZ" \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --location "$AZ_LOCATION_BIZ" \
  --logs-workspace-id "$WS_CUSTOMER_ID" \
  --logs-workspace-key "$WS_SHARED_KEY" 1>/dev/null

echo "Container Apps environment provisioning complete."
