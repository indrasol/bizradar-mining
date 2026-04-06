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

# Sets common secret references/config into all worker apps.
# Required env:
#   AZ_RESOURCE_GROUP_BIZ
#   AZ_CONTAINERAPPS_ENV_BIZ
#   AZ_WORKER_APP_PREFIX_BIZ
# Optional env:
#   AZ_SUBSCRIPTION_ID_BIZ
#   AZ_STORAGE_ACCOUNT_BIZ
#   AZ_STORAGE_CONTAINER_BIZ (default: csv-ingest)
#   SUPABASE_URL_BIZ
#   SUPABASE_SERVICE_KEY_BIZ
#   SUPABASE_ANON_KEY_BIZ
#   OPENAIAPIKEYBIZ
#   AZURE_OPENAI_API_KEY_BIZ
#   AZURE_OPENAI_BASE_URL_BIZ
#   AZURE_OPENAI_DEPLOYMENT_BIZ
#   AZURE_OPENAI_EMBEDDING_MODEL_BIZ

if [[ -n "${AZ_SUBSCRIPTION_ID_BIZ:-}" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION_ID_BIZ"
fi

: "${AZ_RESOURCE_GROUP_BIZ:?AZ_RESOURCE_GROUP_BIZ is required}"
: "${AZ_CONTAINERAPPS_ENV_BIZ:?AZ_CONTAINERAPPS_ENV_BIZ is required}"
: "${AZ_WORKER_APP_PREFIX_BIZ:?AZ_WORKER_APP_PREFIX_BIZ is required}"
AZ_STORAGE_CONTAINER_BIZ="${AZ_STORAGE_CONTAINER_BIZ:-csv-ingest}"
AZURE_STORAGE_ACCOUNT_URL_BIZ="${AZURE_STORAGE_ACCOUNT_URL_BIZ:-}"
if [[ -n "${AZ_STORAGE_ACCOUNT_BIZ:-}" ]]; then
  AZURE_STORAGE_ACCOUNT_URL_BIZ="https://${AZ_STORAGE_ACCOUNT_BIZ}.blob.core.windows.net"
fi

APPS=(
  "${AZ_WORKER_APP_PREFIX_BIZ}-pipeline-core"
  "${AZ_WORKER_APP_PREFIX_BIZ}-classify-enrich"
  "${AZ_WORKER_APP_PREFIX_BIZ}-embedding"
)

for app in "${APPS[@]}"; do
  if ! az containerapp show --resource-group "$AZ_RESOURCE_GROUP_BIZ" --name "$app" 1>/dev/null 2>&1; then
    echo "Skipping missing app: $app"
    continue
  fi
  echo "Configuring secrets/settings for: $app"
  az containerapp secret set \
    --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
    --name "$app" \
    --secrets \
      "supabase-url=${SUPABASE_URL_BIZ:-}" \
      "supabase-service-key=${SUPABASE_SERVICE_KEY_BIZ:-}" \
      "supabase-anon-key=${SUPABASE_ANON_KEY_BIZ:-}" \
      "openai-api-key=${OPENAIAPIKEYBIZ:-}" \
      "azure-openai-api-key=${AZURE_OPENAI_API_KEY_BIZ:-}" 1>/dev/null

  az containerapp update \
    --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
    --name "$app" \
    --set-env-vars \
      "SUPABASE_URL_BIZ=secretref:supabase-url" \
      "SUPABASE_SERVICE_KEY_BIZ=secretref:supabase-service-key" \
      "SUPABASE_ANON_KEY_BIZ=secretref:supabase-anon-key" \
      "OPENAIAPIKEYBIZ=secretref:openai-api-key" \
      "AZURE_OPENAI_API_KEY_BIZ=secretref:azure-openai-api-key" \
      "AZURE_OPENAI_BASE_URL_BIZ=${AZURE_OPENAI_BASE_URL_BIZ:-}" \
      "AZURE_OPENAI_DEPLOYMENT_BIZ=${AZURE_OPENAI_DEPLOYMENT_BIZ:-}" \
      "AZURE_OPENAI_EMBEDDING_MODEL_BIZ=${AZURE_OPENAI_EMBEDDING_MODEL_BIZ:-}" \
      "AZURE_STORAGE_ACCOUNT_URL_BIZ=${AZURE_STORAGE_ACCOUNT_URL_BIZ}" \
      "AZURE_STORAGE_CONTAINER_BIZ=${AZ_STORAGE_CONTAINER_BIZ}" 1>/dev/null
done

echo "Secrets/settings configuration complete."
