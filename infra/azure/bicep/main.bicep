targetScope = 'resourceGroup'

param location string = resourceGroup().location
param logAnalyticsWorkspaceName string = 'bizradar-logs'
param containerAppsEnvironmentName string = 'bizradar-env'
param containerApp1Name string = 'ca-cleaning-and-upsert'
param containerApp2Name string = 'ca-classify-and-summarize'
param containerApp3Name string = 'ca-embedding'
param containerImage string = 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'
param csvEnqueueJobImage string = 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'
param samGovSchedulerImage string = 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'

param storageAccountName string = 'indraproducts'
param keyVaultName string = 'kv-bizradar'

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
  }
}

// 8. Compute: CSV Enqueue Job (event-driven ingestion trigger)
module csvEnqueueJob './modules/compute/csv-enqueue-job.bicep' = {
  name: 'csvEnqueueJob'
  params: {
    location: location
    containerAppsEnvironmentId: foundation.outputs.containerAppsEnvironmentId
    containerImage: csvEnqueueJobImage
    keyVaultName: keyVaultName
  }
}

// 9. Compute: SAM.gov Scheduler Job (scheduled ingestion)
module samGovScheduler './modules/compute/sam-gov-scheduler.bicep' = {
  name: 'samGovScheduler'
  params: {
    location: location
    containerAppsEnvironmentId: foundation.outputs.containerAppsEnvironmentId
    containerImage: samGovSchedulerImage
    keyVaultName: keyVaultName
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
      csvEnqueueJob.outputs.principalId
      samGovScheduler.outputs.principalId
    ]
  }
}
