targetScope = 'resourceGroup'

param location string = resourceGroup().location
param keyVaultName string = 'kv-bizradar'
param containerAppsEnvironmentName string = 'bizradar-env'
param containerImage string = 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'

resource containerAppsEnv 'Microsoft.App/managedEnvironments@2024-03-01' existing = {
  name: containerAppsEnvironmentName
}

module samGovScheduler './modules/compute/sam-gov-scheduler.bicep' = {
  name: 'samGovScheduler'
  params: {
    location: location
    containerAppsEnvironmentId: containerAppsEnv.id
    containerImage: containerImage
    keyVaultName: keyVaultName
  }
}

output jobName string = 'caj-sam-gov-scheduler'
output principalId string = samGovScheduler.outputs.principalId