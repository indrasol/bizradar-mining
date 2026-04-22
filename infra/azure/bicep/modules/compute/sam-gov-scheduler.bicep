param location string
param containerAppsEnvironmentId string
param containerImage string
param keyVaultName string

param cronExpression string = '0 0 * * *'

resource samGovScheduler 'Microsoft.App/jobs@2024-03-01' = {
  name: 'caj-sam-gov-scheduler'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    environmentId: containerAppsEnvironmentId
    configuration: {
      triggerType: 'Schedule'
      scheduleTriggerConfig: {
        cronExpression: cronExpression
        parallelism: 1
        replicaCompletionCount: 1
      }
      replicaTimeout: 7200
      replicaRetryLimit: 1
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
          name: 'sam-api-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/SAM-API-KEY-BIZ'
          identity: 'system'
        }
        {
          name: 'openrag-api-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/OPENRAG-API-KEY-BIZ'
          identity: 'system'
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'sam-gov-pipeline'
          image: containerImage
          resources: {
            cpu: json('2.0')
            memory: '4.0Gi'
          }
          env: [
            {
              name: 'SUPABASE_SERVICE_KEY'
              secretRef: 'supabase-service-key-biz'
            }
            {
              name: 'SAM_API_KEY'
              secretRef: 'sam-api-key-biz'
            }
            {
              name: 'OPENRAG_API_KEY'
              secretRef: 'openrag-api-key-biz'
            }
            {
              name: 'OPENRAG_BASE_URL'
              value: 'https://api.openrag.bizradar.ai'
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
              name: 'SUPABASE_URL'
              value: 'https://fgfehbbljxsuxjdwvmrv.supabase.co'
            }
            {
              name: 'ENV'
              value: 'production'
            }
          ]
        }
      ]
    }
  }
}

output principalId string = samGovScheduler.identity.principalId