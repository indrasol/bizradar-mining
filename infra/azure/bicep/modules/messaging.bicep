param location string

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: 'sb-bizradar'
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
}

var queueNames = [
  'q-csv-chunks'
  'q-raw-rows'
  'q-normalized-rows'
  'q-threaded-rows'
  'q-enriched-rows'
  'q-embedded-rows'
  'q-persist-results'
]

resource queues 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = [for name in queueNames: {
  parent: serviceBusNamespace
  name: name
  properties: {
    maxDeliveryCount: 5
  }
}]

output serviceBusNamespaceName string = serviceBusNamespace.name
