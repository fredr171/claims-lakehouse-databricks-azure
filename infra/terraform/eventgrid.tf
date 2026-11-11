# Landing → Event Grid → storage queue → validation Function.
#
# Event Grid delivers to a queue rather than calling the Function directly: the
# subscription can be created before any Function code is deployed (a Function endpoint
# must already exist), events wait in the queue while the Function is down or being
# redeployed, and failed deliveries go to a dead-letter container instead of vanishing.
resource "azurerm_eventgrid_system_topic" "lake" {
  name                = local.names.event_grid_topic
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  source_resource_id  = azurerm_storage_account.lake.id
  topic_type          = "Microsoft.Storage.StorageAccounts"

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# Event Grid's identity sends to the queue and writes dead letters.
resource "azurerm_role_assignment" "eventgrid_queue_sender" {
  scope                = azurerm_storage_account.func.id
  role_definition_name = "Storage Queue Data Message Sender"
  principal_id         = azurerm_eventgrid_system_topic.lake.identity[0].principal_id
}

resource "azurerm_role_assignment" "eventgrid_deadletter" {
  scope                = azurerm_storage_account.func.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_eventgrid_system_topic.lake.identity[0].principal_id
}

resource "azurerm_eventgrid_system_topic_event_subscription" "landing" {
  name                = "landing-to-validation"
  system_topic        = azurerm_eventgrid_system_topic.lake.name
  resource_group_name = azurerm_resource_group.main.name

  included_event_types = ["Microsoft.Storage.BlobCreated"]
  subject_filter {
    subject_begins_with = "/blobServices/default/containers/landing/"
  }

  # On a hierarchical-namespace account a file is created empty, then written and
  # flushed. Only the calls that finish a file count, so validation never sees a
  # half-written file.
  advanced_filter {
    string_in {
      key    = "data.api"
      values = ["PutBlob", "PutBlockList", "CopyBlob", "FlushWithClose"]
    }
  }

  delivery_identity {
    type = "SystemAssigned"
  }

  storage_queue_endpoint {
    storage_account_id = azurerm_storage_account.func.id
    queue_name         = azurerm_storage_queue.landing_events.name
  }

  dead_letter_identity {
    type = "SystemAssigned"
  }

  storage_blob_dead_letter_destination {
    storage_account_id          = azurerm_storage_account.func.id
    storage_blob_container_name = azurerm_storage_container.event_deadletter.name
  }

  retry_policy {
    max_delivery_attempts = 30
    event_time_to_live    = 1440
  }

  depends_on = [
    azurerm_role_assignment.eventgrid_queue_sender,
    azurerm_role_assignment.eventgrid_deadletter,
  ]
}
