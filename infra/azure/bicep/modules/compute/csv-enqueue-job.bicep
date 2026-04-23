param location string
param containerAppsEnvironmentId string
param containerImage string
param keyVaultName string

resource csvEnqueueJob 'Microsoft.App/jobs@2024-03-01' = {
  name: 'caj-csv-enqueue'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    environmentId: containerAppsEnvironmentId
    configuration: {
      // Manual trigger — started on-demand via GitHub Actions or az CLI.
      // For a fully autonomous daily schedule, change triggerType to 'Schedule'
      // and add scheduleTriggerConfig: { cronExpression: '0 8 * * *' }.
      triggerType: 'Manual'
      replicaTimeout: 7200   // 2 hours: covers Playwright download + blob upload
      replicaRetryLimit: 0   // fail fast — do not auto-retry on error
      registries: [
        {
          server: 'securetrack-dzc8a3deejhje7d4.azurecr.io'
          identity: 'system'
        }
      ]
      secrets: [
        {
          name: 'supabase-service-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/SUPABASE-SERVICE-KEY-BIZ'
          identity: 'system'
        }
        {
          name: 'azure-openai-api-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/AZURE-OPENAI-API-KEY-BIZ'
          identity: 'system'
        }
        {
          name: 'sam-api-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/SAM-API-KEY-BIZ'
          identity: 'system'
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'csv-enqueue'
          image: containerImage
          resources: {
            // 1 vCPU and 2 Gi — extra memory required for Playwright/Chromium
            cpu: json('1.0')
            memory: '2.0Gi'
          }
          env: [
            {
              name: 'SUPABASE_SERVICE_KEY_BIZ'
              secretRef: 'supabase-service-key-biz'
            }
            {
              name: 'AZURE_OPENAI_API_KEY_BIZ'
              secretRef: 'azure-openai-api-key-biz'
            }
            {
              name: 'SAM_API_KEY_BIZ'
              secretRef: 'sam-api-key-biz'
            }
            {
              name: 'AZURE_KEYVAULT_NAME_BIZ'
              value: keyVaultName
            }
            {
              name: 'AZURE_SERVICEBUS_FQNS_BIZ'
              value: 'sb-bizradar.servicebus.windows.net'
            }
            {
              name: 'AZURE_STORAGE_ACCOUNT_URL_BIZ'
              value: 'https://indraproducts.blob.core.windows.net'
            }
            {
              name: 'AZURE_STORAGE_CONTAINER_BIZ'
              value: 'csv-ingest'
            }
            {
              name: 'SUPABASE_URL_BIZ'
              value: 'https://fgfehbbljxsuxjdwvmrv.supabase.co'
            }
            {
              name: 'ENV_BIZ'
              value: 'production'
            }
            // CSV_PATH controls where data_extraction.py saves the downloaded file.
            // Can be overridden per-execution via --env-vars in az containerapp job start.
            {
              name: 'CSV_PATH'
              value: '/app/ContractOpportunitiesFullCSV.csv'
            }
            // CSV_CHUNK_SIZE_BIZ and CSV_LIMIT_BIZ can be overridden at runtime
            // via: az containerapp job start --env-vars CSV_CHUNK_SIZE_BIZ=500
            {
              name: 'CSV_CHUNK_SIZE_BIZ'
              value: '500'
            }
          ]
        }
      ]
    }
  }
}

output principalId string = csvEnqueueJob.identity.principalId

