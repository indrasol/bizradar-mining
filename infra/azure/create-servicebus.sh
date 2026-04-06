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

# Creates Service Bus namespace + pipeline queues.
# Required env:
#   AZ_RESOURCE_GROUP_BIZ
#   AZ_LOCATION_BIZ
#   AZ_SERVICEBUS_NAMESPACE_BIZ
# Optional env:
#   AZ_SUBSCRIPTION_ID_BIZ
#   SB_SKU (default: Standard)
#   SB_QUEUE_LOCK_SECONDS (default: 120)
#   SB_QUEUE_MAX_DELIVERY (default: 10)
#   SB_QUEUE_TTL_ISO8601 (default: P14D)
#   SB_QUEUE_MAX_SIZE_MB (default: 1024)

if [[ -n "${AZ_SUBSCRIPTION_ID_BIZ:-}" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION_ID_BIZ"
fi

: "${AZ_RESOURCE_GROUP_BIZ:?AZ_RESOURCE_GROUP_BIZ is required}"
: "${AZ_LOCATION_BIZ:?AZ_LOCATION_BIZ is required}"
: "${AZ_SERVICEBUS_NAMESPACE_BIZ:?AZ_SERVICEBUS_NAMESPACE_BIZ is required}"

SB_SKU="${SB_SKU:-Standard}"
SB_QUEUE_LOCK_SECONDS="${SB_QUEUE_LOCK_SECONDS:-120}"
SB_QUEUE_MAX_DELIVERY="${SB_QUEUE_MAX_DELIVERY:-10}"
SB_QUEUE_TTL_ISO8601="${SB_QUEUE_TTL_ISO8601:-P14D}"
SB_QUEUE_MAX_SIZE_MB="${SB_QUEUE_MAX_SIZE_MB:-1024}"

QUEUE_NAMES=(
  "q-csv-chunks"
  "q-raw-rows"
  "q-normalized-rows"
  "q-threaded-rows"
  "q-enriched-rows"
  "q-embedded-rows"
  "q-persist-results"
)

echo "Ensuring resource group exists: $AZ_RESOURCE_GROUP_BIZ"
az group create --name "$AZ_RESOURCE_GROUP_BIZ" --location "$AZ_LOCATION_BIZ" 1>/dev/null

echo "Ensuring service bus namespace exists: $AZ_SERVICEBUS_NAMESPACE_BIZ"
az servicebus namespace create \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --name "$AZ_SERVICEBUS_NAMESPACE_BIZ" \
  --location "$AZ_LOCATION_BIZ" \
  --sku "$SB_SKU" 1>/dev/null

for queue in "${QUEUE_NAMES[@]}"; do
  echo "Ensuring queue exists: $queue"
  az servicebus queue create \
    --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
    --namespace-name "$AZ_SERVICEBUS_NAMESPACE_BIZ" \
    --name "$queue" \
    --lock-duration "PT${SB_QUEUE_LOCK_SECONDS}S" \
    --max-delivery-count "$SB_QUEUE_MAX_DELIVERY" \
    --default-message-time-to-live "$SB_QUEUE_TTL_ISO8601" \
    --max-size "$SB_QUEUE_MAX_SIZE_MB" \
    --enable-dead-lettering-on-message-expiration true 1>/dev/null
done

echo "Service Bus provisioning complete."
