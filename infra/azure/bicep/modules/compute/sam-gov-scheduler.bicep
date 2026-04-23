param location string
param containerAppsEnvironmentId string
param containerImage string
param keyVaultName string

param acrServer string = 'securetrack-dzc8a3deejhje7d4.azurecr.io'
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
          server: acrServer
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
      ]
    }
    template: {
      containers: [
        {
          name: 'sam-gov-pipeline'
          image: containerImage
          command: ['python']
          args: ['-m', 'sam_gov.services.acj_cron.main']
          resources: {
            cpu: json('2.0')
            memory: '4.0Gi'
          }
          env: [
            {
              name: 'SUPABASE_SERVICE_KEY_BIZ'
              secretRef: 'supabase-service-key-biz'
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
              name: 'SUPABASE_TABLE'
              value: 'documents'
            }
            {
              name: 'SUPABASE_NOTICE_ID_COLUMN'
              value: 'notice_id'
            }
            {
              name: 'OPENRAG_BASE_URL'
              value: 'http://ca-openrag-backend'
            }
            {
              name: 'ENV_BIZ'
              value: 'production'
            }
            {
              name: 'CSV_PATH'
              value: '/app/ContractOpportunitiesFullCSV.csv'
            }
            {
              name: 'ROWS_PER_CHUNK'
              value: '500'
            }
            {
              name: 'INGEST_MAX_CONCURRENCY'
              value: '5'
            }
            {
              name: 'TASK_POLL_INTERVAL_SECONDS'
              value: '10'
            }
            {
              name: 'TASK_POLL_TIMEOUT_SECONDS'
              value: '600'
            }
            {
              name: 'INGEST_MAX_RETRIES'
              value: '3'
            }
          ]
        }
      ]
    }
  }
}

output principalId string = samGovScheduler.identity.principalId