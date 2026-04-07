targetScope = 'resourceGroup'

param location string = resourceGroup().location
param logAnalyticsWorkspaceName string = 'bizradar-logs'
param containerAppsEnvironmentName string = 'bizradar-env'
param containerApp1Name string = 'ca-cleaning-and-upsert'
param containerApp2Name string = 'ca-classify-and-summarize'
param containerApp3Name string = 'ca-embedding'
param containerImage string = 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'

param storageAccountName string = 'indraproducts'
param keyVaultName string = 'kv-bizradar'

@secure()
param supabaseServiceKey string = ''
@secure()
param supabaseAnonKey string = ''

// 1. Foundation: Logs + Cluster
module foundation './modules/foundation.bicep' = {
  name: 'foundation'
  params: {
    location: location
    logAnalyticsWorkspaceName: logAnalyticsWorkspaceName
    containerAppsEnvironmentName: containerAppsEnvironmentName
  }
}

// 2. Messaging: Service Bus + Queues
module messaging './modules/messaging.bicep' = {
  name: 'messaging'
  params: {
    location: location
  }
}

// 3. Storage: Blob storage
module storage './modules/storage.bicep' = {
  name: 'storage'
  params: {
    storageAccountName: storageAccountName
  }
}

// 5. Compute: Container 1 Worker
module container1 './modules/compute/container1.bicep' = {
  name: 'container1'
  params: {
    location: location
    containerAppName: containerApp1Name
    containerAppsEnvironmentId: foundation.outputs.containerAppsEnvironmentId
    containerImage: containerImage
    keyVaultName: keyVaultName
    supabaseServiceKey: supabaseServiceKey
    supabaseAnonKey: supabaseAnonKey
  }
}

// 6. Compute: Container 2 Worker
module container2 './modules/compute/container2.bicep' = {
  name: 'container2'
  params: {
    location: location
    containerAppName: containerApp2Name
    containerAppsEnvironmentId: foundation.outputs.containerAppsEnvironmentId
    containerImage: containerImage
    keyVaultName: keyVaultName
    supabaseServiceKey: supabaseServiceKey
    supabaseAnonKey: supabaseAnonKey
  }
}

// 7. Compute: Container 3 Worker
module container3 './modules/compute/container3.bicep' = {
  name: 'container3'
  params: {
    location: location
    containerAppName: containerApp3Name
    containerAppsEnvironmentId: foundation.outputs.containerAppsEnvironmentId
    containerImage: containerImage
    keyVaultName: keyVaultName
    supabaseServiceKey: supabaseServiceKey
    supabaseAnonKey: supabaseAnonKey
  }
}

// 4. Security: Managed Identity + Role Assignments
module security './modules/security.bicep' = {
  name: 'security'
  params: {
    location: location
    principalIds: [
      container1.outputs.principalId
      container2.outputs.principalId
      container3.outputs.principalId
    ]
  }
}
