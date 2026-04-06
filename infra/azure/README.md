# Azure Provisioning Scripts (CLI)

These scripts provision and deploy the Azure queue pipeline.

## Scripts

- `create-servicebus.sh`
- `create-storage.sh`
- `create-container-apps-env.sh`
- `configure-identities-rbac.sh`
- `configure-secrets.sh`
- `configure-observability.sh`
- `deploy-worker-apps.sh`

## Current topology (3 Container Apps)

- `${AZ_WORKER_APP_PREFIX_BIZ}-pipeline-core`
  - Runs `chunk_reader`, `normalize_dedupe`, `thread_match`, `persist`, `run_finalizer`
- `${AZ_WORKER_APP_PREFIX_BIZ}-classify-enrich`
  - Runs `classify_enrich`
- `${AZ_WORKER_APP_PREFIX_BIZ}-embedding`
  - Runs `embedding`

Service Bus queues are unchanged:

- `q-csv-chunks` -> `q-raw-rows` -> `q-normalized-rows` -> `q-threaded-rows`
- `q-threaded-rows` -> `q-enriched-rows` -> `q-embedded-rows` -> `q-persist-results`

## Prerequisites

- Azure CLI logged in (`az login`)
- Correct subscription selected or `AZ_SUBSCRIPTION_ID_BIZ` exported
- Access to target resource group or permission to create it
- Existing ACR for image publishing (used by deploy workflow)
- Local script runs auto-load `backend/.env` when present (values can still be overridden by exported env vars)

## Recommended order

```bash
infra/azure/create-servicebus.sh
infra/azure/create-storage.sh
infra/azure/create-container-apps-env.sh
infra/azure/configure-identities-rbac.sh
infra/azure/configure-observability.sh
infra/azure/deploy-worker-apps.sh
infra/azure/configure-secrets.sh
```

## Required environment variables

- `AZ_RESOURCE_GROUP_BIZ`
- `AZ_LOCATION_BIZ`
- `AZ_SERVICEBUS_NAMESPACE_BIZ`
- `AZ_STORAGE_ACCOUNT_BIZ`
- `AZ_CONTAINERAPPS_ENV_BIZ`
- `AZ_LOG_ANALYTICS_WS_BIZ`
- `AZ_APP_INSIGHTS_NAME_BIZ`
- `AZ_WORKER_IDENTITY_NAME_BIZ`
- `AZ_WORKER_APP_PREFIX_BIZ`
- `AZ_CONTAINER_IMAGE_BIZ` (for `deploy-worker-apps.sh`)

Optional:

- `AZ_SUBSCRIPTION_ID_BIZ`
- `AZ_STORAGE_CONTAINER_BIZ` (default `csv-ingest`)
- `AZ_KEYVAULT_NAME_BIZ`

Deployment toggles:

- None. Deployment is fixed to the 3-container topology.

Scaling knobs:

- `AZ_CORE_APP_MIN_REPLICAS_BIZ`, `AZ_CORE_APP_MAX_REPLICAS_BIZ`
- `AZ_CLASSIFY_APP_MIN_REPLICAS_BIZ`, `AZ_CLASSIFY_APP_MAX_REPLICAS_BIZ`
- `AZ_EMBED_APP_MIN_REPLICAS_BIZ`, `AZ_EMBED_APP_MAX_REPLICAS_BIZ`

## Rollout notes

1. Deploy new image and 3-app topology.
2. Validate processing health:
   - Queue depth/age stable
   - DLQ counts not rising abnormally
   - Error rates comparable to baseline

## Workflow trigger policy

- `deploy-azure-queue-workers.yml`: manual only (`workflow_dispatch`)
- `sam-csv-extract-enqueue.yml`: scheduled daily at 12:00 AM PST (UTC cron) + manual dispatch
- Push-triggered workflows should remain validation-only (no infra deployment)
