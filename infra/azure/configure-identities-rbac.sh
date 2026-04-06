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

# Creates one user-assigned managed identity for pipeline workers and grants RBAC.
# Required env:
#   AZ_RESOURCE_GROUP_BIZ
#   AZ_WORKER_IDENTITY_NAME_BIZ
#   AZ_SERVICEBUS_NAMESPACE_BIZ
#   AZ_STORAGE_ACCOUNT_BIZ
# Optional env:
#   AZ_SUBSCRIPTION_ID_BIZ
#   AZ_KEYVAULT_NAME_BIZ

if [[ -n "${AZ_SUBSCRIPTION_ID_BIZ:-}" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION_ID_BIZ"
fi

: "${AZ_RESOURCE_GROUP_BIZ:?AZ_RESOURCE_GROUP_BIZ is required}"
: "${AZ_WORKER_IDENTITY_NAME_BIZ:?AZ_WORKER_IDENTITY_NAME_BIZ is required}"
: "${AZ_SERVICEBUS_NAMESPACE_BIZ:?AZ_SERVICEBUS_NAMESPACE_BIZ is required}"
: "${AZ_STORAGE_ACCOUNT_BIZ:?AZ_STORAGE_ACCOUNT_BIZ is required}"

echo "Ensuring user-assigned managed identity exists: $AZ_WORKER_IDENTITY_NAME_BIZ"
az identity create \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --name "$AZ_WORKER_IDENTITY_NAME_BIZ" 1>/dev/null

WORKER_PRINCIPAL_ID="$(az identity show \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --name "$AZ_WORKER_IDENTITY_NAME_BIZ" \
  --query principalId -o tsv)"

SB_ID="$(az servicebus namespace show \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --name "$AZ_SERVICEBUS_NAMESPACE_BIZ" \
  --query id -o tsv)"

STORAGE_ID="$(az storage account show \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --name "$AZ_STORAGE_ACCOUNT_BIZ" \
  --query id -o tsv)"

echo "Granting Service Bus Data Owner role to worker identity"
az role assignment create \
  --assignee-object-id "$WORKER_PRINCIPAL_ID" \
  --assignee-principal-type ServicePrincipal \
  --role "Azure Service Bus Data Owner" \
  --scope "$SB_ID" 1>/dev/null || true

echo "Granting Storage Blob Data Contributor role to worker identity"
az role assignment create \
  --assignee-object-id "$WORKER_PRINCIPAL_ID" \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_ID" 1>/dev/null || true

if [[ -n "${AZ_KEYVAULT_NAME_BIZ:-}" ]]; then
  KV_ID="$(az keyvault show \
    --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
    --name "$AZ_KEYVAULT_NAME_BIZ" \
    --query id -o tsv)"
  echo "Granting Key Vault Secrets User role to worker identity"
  az role assignment create \
    --assignee-object-id "$WORKER_PRINCIPAL_ID" \
    --assignee-principal-type ServicePrincipal \
    --role "Key Vault Secrets User" \
    --scope "$KV_ID" 1>/dev/null || true
fi

echo "Identity and RBAC configuration complete."
