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

# Deploys/updates container app workers.
# Required env:
#   AZ_RESOURCE_GROUP_BIZ
#   AZ_CONTAINERAPPS_ENV_BIZ
#   AZ_WORKER_APP_PREFIX_BIZ
#   AZ_CONTAINER_IMAGE_BIZ
# Optional env:
#   AZ_SUBSCRIPTION_ID_BIZ
#   AZ_WORKER_IDENTITY_NAME_BIZ
#   AZ_SERVICEBUS_NAMESPACE_BIZ
#   AZ_STORAGE_CONTAINER_BIZ (default: csv-ingest)
#   AZ_CORE_APP_MIN_REPLICAS_BIZ (default 1)
#   AZ_CORE_APP_MAX_REPLICAS_BIZ (default 10)
#   AZ_CLASSIFY_APP_MIN_REPLICAS_BIZ (default 0)
#   AZ_CLASSIFY_APP_MAX_REPLICAS_BIZ (default 6)
#   AZ_EMBED_APP_MIN_REPLICAS_BIZ (default 0)
#   AZ_EMBED_APP_MAX_REPLICAS_BIZ (default 4)

if [[ -n "${AZ_SUBSCRIPTION_ID_BIZ:-}" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION_ID_BIZ"
fi

: "${AZ_RESOURCE_GROUP_BIZ:?AZ_RESOURCE_GROUP_BIZ is required}"
: "${AZ_CONTAINERAPPS_ENV_BIZ:?AZ_CONTAINERAPPS_ENV_BIZ is required}"
: "${AZ_WORKER_APP_PREFIX_BIZ:?AZ_WORKER_APP_PREFIX_BIZ is required}"
: "${AZ_CONTAINER_IMAGE_BIZ:?AZ_CONTAINER_IMAGE_BIZ is required}"
: "${AZ_SERVICEBUS_NAMESPACE_BIZ:?AZ_SERVICEBUS_NAMESPACE_BIZ is required}"
: "${AZ_STORAGE_ACCOUNT_BIZ:?AZ_STORAGE_ACCOUNT_BIZ is required}"

AZ_STORAGE_CONTAINER_BIZ="${AZ_STORAGE_CONTAINER_BIZ:-csv-ingest}"
AZURE_STORAGE_ACCOUNT_URL_BIZ="https://${AZ_STORAGE_ACCOUNT_BIZ}.blob.core.windows.net"

AZ_CORE_APP_MIN_REPLICAS_BIZ="${AZ_CORE_APP_MIN_REPLICAS_BIZ:-1}"
AZ_CORE_APP_MAX_REPLICAS_BIZ="${AZ_CORE_APP_MAX_REPLICAS_BIZ:-10}"
AZ_CLASSIFY_APP_MIN_REPLICAS_BIZ="${AZ_CLASSIFY_APP_MIN_REPLICAS_BIZ:-0}"
AZ_CLASSIFY_APP_MAX_REPLICAS_BIZ="${AZ_CLASSIFY_APP_MAX_REPLICAS_BIZ:-6}"
AZ_EMBED_APP_MIN_REPLICAS_BIZ="${AZ_EMBED_APP_MIN_REPLICAS_BIZ:-0}"
AZ_EMBED_APP_MAX_REPLICAS_BIZ="${AZ_EMBED_APP_MAX_REPLICAS_BIZ:-4}"

IDENTITY_ARG=""
if [[ -n "${AZ_WORKER_IDENTITY_NAME_BIZ:-}" ]]; then
  IDENTITY_ID="$(az identity show --resource-group "$AZ_RESOURCE_GROUP_BIZ" --name "$AZ_WORKER_IDENTITY_NAME_BIZ" --query id -o tsv)"
  IDENTITY_ARG="--user-assigned $IDENTITY_ID"
fi

create_or_update_worker() {
  local app_name="$1"
  local worker_name="$2"
  local in_queue="$3"
  local out_queue="$4"
  local min_replicas="$5"
  local max_replicas="$6"
  local cpu="$7"
  local memory="$8"

  if az containerapp show --resource-group "$AZ_RESOURCE_GROUP_BIZ" --name "$app_name" 1>/dev/null 2>&1; then
    echo "Updating container app: $app_name"
    az containerapp update \
      --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
      --name "$app_name" \
      --image "$AZ_CONTAINER_IMAGE_BIZ" \
      --set-env-vars \
        "SUPABASE_SERVICE_KEY_BIZ=${SUPABASE_SERVICE_KEY_BIZ:-}" \
        "SUPABASE_ANON_KEY_BIZ=${SUPABASE_ANON_KEY_BIZ:-}" \
        "PIPELINE_WORKER_NAME_BIZ=$worker_name" \
        "SERVICEBUS_NAMESPACE_BIZ=${AZ_SERVICEBUS_NAMESPACE_BIZ}.servicebus.windows.net" \
        "PIPELINE_INPUT_QUEUE_BIZ=$in_queue" \
        "PIPELINE_OUTPUT_QUEUE_BIZ=$out_queue" \
        "AZURE_SERVICEBUS_FQNS_BIZ=${AZ_SERVICEBUS_NAMESPACE_BIZ}.servicebus.windows.net" \
        "AZURE_STORAGE_ACCOUNT_URL_BIZ=${AZURE_STORAGE_ACCOUNT_URL_BIZ}" \
        "AZURE_STORAGE_CONTAINER_BIZ=${AZ_STORAGE_CONTAINER_BIZ}" 1>/dev/null
  else
    echo "Creating container app: $app_name"
    az containerapp create \
      --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
      --name "$app_name" \
      --environment "$AZ_CONTAINERAPPS_ENV_BIZ" \
      --image "$AZ_CONTAINER_IMAGE_BIZ" \
      --cpu "$cpu" \
      --memory "$memory" \
      --min-replicas "$min_replicas" \
      --max-replicas "$max_replicas" \
      --command "python" \
      --args "-m sam_gov.workers.queue_pipeline.run_worker --worker $worker_name" \
      --env-vars \
        "SUPABASE_SERVICE_KEY_BIZ=${SUPABASE_SERVICE_KEY_BIZ:-}" \
        "SUPABASE_ANON_KEY_BIZ=${SUPABASE_ANON_KEY_BIZ:-}" \
        "PIPELINE_WORKER_NAME_BIZ=$worker_name" \
        "SERVICEBUS_NAMESPACE_BIZ=${AZ_SERVICEBUS_NAMESPACE_BIZ}.servicebus.windows.net" \
        "PIPELINE_INPUT_QUEUE_BIZ=$in_queue" \
        "PIPELINE_OUTPUT_QUEUE_BIZ=$out_queue" \
        "AZURE_SERVICEBUS_FQNS_BIZ=${AZ_SERVICEBUS_NAMESPACE_BIZ}.servicebus.windows.net" \
        "AZURE_STORAGE_ACCOUNT_URL_BIZ=${AZURE_STORAGE_ACCOUNT_URL_BIZ}" \
        "AZURE_STORAGE_CONTAINER_BIZ=${AZ_STORAGE_CONTAINER_BIZ}" \
      ${IDENTITY_ARG} 1>/dev/null
  fi
}

deploy_three_app_topology() {
  create_or_update_worker \
    "${AZ_WORKER_APP_PREFIX_BIZ}-pipeline-core" \
    "pipeline_core" \
    "-" \
    "-" \
    "$AZ_CORE_APP_MIN_REPLICAS_BIZ" \
    "$AZ_CORE_APP_MAX_REPLICAS_BIZ" \
    "1.0" \
    "2.0Gi"
  create_or_update_worker \
    "${AZ_WORKER_APP_PREFIX_BIZ}-classify-enrich" \
    "classify_enrich" \
    "q-threaded-rows" \
    "q-enriched-rows" \
    "$AZ_CLASSIFY_APP_MIN_REPLICAS_BIZ" \
    "$AZ_CLASSIFY_APP_MAX_REPLICAS_BIZ" \
    "0.75" \
    "1.5Gi"
  create_or_update_worker \
    "${AZ_WORKER_APP_PREFIX_BIZ}-embedding" \
    "embedding" \
    "q-enriched-rows" \
    "q-embedded-rows" \
    "$AZ_EMBED_APP_MIN_REPLICAS_BIZ" \
    "$AZ_EMBED_APP_MAX_REPLICAS_BIZ" \
    "0.75" \
    "1.5Gi"
}

deploy_three_app_topology

echo "Worker app deployment complete."
