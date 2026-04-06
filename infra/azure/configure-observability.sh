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

# Creates Application Insights and wires basic diagnostics.
# Required env:
#   AZ_RESOURCE_GROUP_BIZ
#   AZ_LOCATION_BIZ
#   AZ_APP_INSIGHTS_NAME_BIZ
# Optional env:
#   AZ_SUBSCRIPTION_ID_BIZ

if [[ -n "${AZ_SUBSCRIPTION_ID_BIZ:-}" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION_ID_BIZ"
fi

: "${AZ_RESOURCE_GROUP_BIZ:?AZ_RESOURCE_GROUP_BIZ is required}"
: "${AZ_LOCATION_BIZ:?AZ_LOCATION_BIZ is required}"
: "${AZ_APP_INSIGHTS_NAME_BIZ:?AZ_APP_INSIGHTS_NAME_BIZ is required}"

echo "Ensuring Application Insights exists: $AZ_APP_INSIGHTS_NAME_BIZ"
az monitor app-insights component create \
  --app "$AZ_APP_INSIGHTS_NAME_BIZ" \
  --location "$AZ_LOCATION_BIZ" \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --application-type web 1>/dev/null

CONNECTION_STRING="$(az monitor app-insights component show \
  --app "$AZ_APP_INSIGHTS_NAME_BIZ" \
  --resource-group "$AZ_RESOURCE_GROUP_BIZ" \
  --query connectionString -o tsv)"

echo "Application Insights connection string generated."
echo "Set APPLICATIONINSIGHTS_CONNECTION_STRING secret/value in worker apps to enable tracing."
echo "Value (masked manually in CI secrets): $CONNECTION_STRING"
