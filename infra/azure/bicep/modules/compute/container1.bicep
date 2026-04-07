param location string
param containerAppName string
param containerAppsEnvironmentId string
param containerImage string
param keyVaultName string
@secure()
param supabaseServiceKey string = ''

@secure()
param supabaseAnonKey string = ''

resource app 'Microsoft.App/containerApps@2025-01-01' = {
  name: containerAppName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: containerAppsEnvironmentId
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [
        {
          server: 'securetrack-dzc8a3deejhje7d4.azurecr.io'
          identity: 'system'
        }
      ]
      secrets: [
        {
          name: 'supabase-service-key-biz'
          value: supabaseServiceKey
        }
        {
          name: 'supabase-anon-key-biz'
          value: supabaseAnonKey
        }
        {
          name: 'supabase-jwt-secret-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/SUPABASE-JWT-SECRET-BIZ'
          identity: 'system'
        }
        {
          name: 'azure-openai-api-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/AZURE-OPENAI-API-KEY-BIZ'
          identity: 'system'
        }
        {
          name: 'db-password-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/DB-PASSWORD-BIZ'
          identity: 'system'
        }
        {
          name: 'redis-password-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/REDIS-PASSWORD-BIZ'
          identity: 'system'
        }
        {
          name: 'stripe-secret-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/STRIPE-SECRET-KEY-BIZ'
          identity: 'system'
        }
        {
          name: 'stripe-webhook-secret-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/STRIPE-WEBHOOK-SECRET-BIZ'
          identity: 'system'
        }
        {
          name: 'sam-api-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/SAM-API-KEY-BIZ'
          identity: 'system'
        }
        {
          name: 'pinecone-api-key-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/PINECONE-API-KEY-BIZ'
          identity: 'system'
        }
        {
          name: 'github-token-biz'
          keyVaultUrl: 'https://${keyVaultName}.vault.azure.net/secrets/GITHUB-TOKEN-BIZ'
          identity: 'system'
        }
      ]
    }
    template: {
      containers: [
        {
          name: containerAppName
          image: containerImage
          command: ['python']
          args: ['-m', 'sam_gov.workers.queue_pipeline.run_worker', '--worker', 'pipeline_core']
          env: [
            {
              name: 'SUPABASE_SERVICE_KEY_BIZ'
              secretRef: 'supabase-service-key-biz'
            }
            {
              name: 'SUPABASE_ANON_KEY_BIZ'
              secretRef: 'supabase-anon-key-biz'
            }
            {
              name: 'SUPABASE_JWT_SECRET_BIZ'
              secretRef: 'supabase-jwt-secret-biz'
            }
            {
              name: 'AZURE_OPENAI_API_KEY_BIZ'
              secretRef: 'azure-openai-api-key-biz'
            }
            {
              name: 'DB_PASSWORD_BIZ'
              secretRef: 'db-password-biz'
            }
            {
              name: 'REDIS_PASSWORD_BIZ'
              secretRef: 'redis-password-biz'
            }
            {
              name: 'STRIPE_SECRET_KEY_BIZ'
              secretRef: 'stripe-secret-key-biz'
            }
            {
              name: 'STRIPE_WEBHOOK_SECRET_BIZ'
              secretRef: 'stripe-webhook-secret-biz'
            }
            {
              name: 'SAM_API_KEY_BIZ'
              secretRef: 'sam-api-key-biz'
            }
            {
              name: 'PINECONE_API_KEY_BIZ'
              secretRef: 'pinecone-api-key-biz'
            }
            {
              name: 'GITHUB_TOKEN_BIZ'
              secretRef: 'github-token-biz'
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
            // --- Non-Sensitive Configuration ---
            {
              name: 'ENV_BIZ'
              value: 'production'
            }
            {
              name: 'SUPABASE_URL_BIZ'
              value: 'https://fgfehbbljxsuxjdwvmrv.supabase.co'
            }
            {
              name: 'DB_HOST_BIZ'
              value: 'db.fgfehbbljxsuxjdwvmrv.supabase.co'
            }
            {
              name: 'DB_PORT_BIZ'
              value: '5432'
            }
            {
              name: 'DB_NAME_BIZ'
              value: 'postgres'
            }
            {
              name: 'DB_USER_BIZ'
              value: 'postgres.fgfehbbljxsuxjdwvmrv'
            }
            {
              name: 'REDIS_HOST_BIZ'
              value: 'redis-14454.c57.us-east-1-4.ec2.redns.redis-cloud.com'
            }
            {
              name: 'REDIS_PORT_BIZ'
              value: '14454'
            }
            {
              name: 'REDIS_USERNAME_BIZ'
              value: 'Admin'
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1.0Gi'
          }
        }
      ]
      scale: {
        minReplicas: 0
        maxReplicas: 10
      }
    }
  }
}

output principalId string = app.identity.principalId
