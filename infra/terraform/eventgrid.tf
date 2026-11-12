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

# Archive → Event Grid → storage queue → Databricks file events (the job's file-arrival
# trigger). Terraform provides the queue and subscription, so Databricks only reads the
# queue: in its automatic mode it creates them itself, which needs management roles on the
# lake account and did not finish on an account with shared-key access disabled.
resource "azurerm_storage_queue" "archive_events" {
  name               = local.archive_events_queue
  storage_account_id = azurerm_storage_account.lake.id
}

# On the account, not the queue: Event Grid's delivery check rejects a queue-scoped role
# ("Managed Identity Authorization Error"). The archive queue is the lake's only queue.
resource "azurerm_role_assignment" "eventgrid_archive_queue_sender" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Queue Data Message Sender"
  principal_id         = azurerm_eventgrid_system_topic.lake.identity[0].principal_id
}

resource "azurerm_eventgrid_system_topic_event_subscription" "archive" {
  name                = "archive-to-databricks"
  system_topic        = azurerm_eventgrid_system_topic.lake.name
  resource_group_name = azurerm_resource_group.main.name

  # The event types Databricks file events read. No API filter: Databricks decides which
  # calls finish a file.
  included_event_types = [
    "Microsoft.Storage.BlobCreated",
    "Microsoft.Storage.BlobDeleted",
    "Microsoft.Storage.BlobRenamed",
    "Microsoft.Storage.DirectoryDeleted",
    "Microsoft.Storage.DirectoryRenamed",
  ]
  subject_filter {
    subject_begins_with = "/blobServices/default/containers/archive/"
  }

  delivery_identity {
    type = "SystemAssigned"
  }

  storage_queue_endpoint {
    storage_account_id = azurerm_storage_account.lake.id
    queue_name         = azurerm_storage_queue.archive_events.name
  }

  retry_policy {
    max_delivery_attempts = 30
    event_time_to_live    = 1440
  }

  depends_on = [azurerm_role_assignment.eventgrid_archive_queue_sender]
}
