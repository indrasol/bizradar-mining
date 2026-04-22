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
        }
      ]
    }
  }
}

output principalId string = samGovScheduler.identity.principalId