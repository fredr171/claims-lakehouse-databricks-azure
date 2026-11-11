# The validation Function: checks every file that lands, then copies it to archive or
# quarantine. Flex Consumption (FC1) bills per execution and scales to zero, and supports
# managed identity for its own storage, so no storage keys are needed anywhere.

# The Function's own storage: deployment package, the landing-events queue and the
# Event Grid dead-letter container. Separate from the lake so runtime files never mix
# with data.
resource "azurerm_storage_account" "func" {
  name                     = local.names.func_storage
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  default_to_oauth_authentication = true
  local_user_enabled              = false

  tags = local.tags
}

resource "azurerm_storage_container" "func_deploy" {
  name                  = local.func_deploy_container
  storage_account_id    = azurerm_storage_account.func.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "event_deadletter" {
  name                  = local.event_deadletter_container
  storage_account_id    = azurerm_storage_account.func.id
  container_access_type = "private"
}

# Event Grid delivers each landing event here; the Function's queue trigger reads it.
resource "azurerm_storage_queue" "landing_events" {
  name               = local.landing_events_queue
  storage_account_id = azurerm_storage_account.func.id
}

resource "azurerm_service_plan" "validation" {
  name                = local.names.function_plan
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = local.tags
}

resource "azurerm_function_app_flex_consumption" "validation" {
  name                = local.names.validation_function
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  service_plan_id     = azurerm_service_plan.validation.id

  storage_container_type      = "blobContainer"
  storage_container_endpoint  = "${azurerm_storage_account.func.primary_blob_endpoint}${azurerm_storage_container.func_deploy.name}"
  storage_authentication_type = "SystemAssignedIdentity"

  # Python 3.12 matches the repo's local environment and Databricks serverless.
  runtime_name           = "python"
  runtime_version        = "3.12"
  maximum_instance_count = 40
  instance_memory_in_mb  = 2048
  https_only             = true

  identity {
    type = "SystemAssigned"
  }

  app_settings = {
    # Identity-based connection to the Function's own storage (queue trigger, host state).
    "AzureWebJobsStorage__accountName" = azurerm_storage_account.func.name

    LANDING_EVENTS_QUEUE = local.landing_events_queue
    LAKE_BLOB_ENDPOINT   = azurerm_storage_account.lake.primary_blob_endpoint
    LANDING_CONTAINER    = "landing"
    ARCHIVE_CONTAINER    = "archive"
    QUARANTINE_CONTAINER = "quarantine"
  }

  site_config {
    application_insights_connection_string = azurerm_application_insights.main.connection_string
  }

  tags = local.tags
}

# The Function's identity on its own storage: deployment package and host state (blob),
# queue trigger (receive, delete, poison queue).
resource "azurerm_role_assignment" "validation_func_blob" {
  scope                = azurerm_storage_account.func.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azurerm_function_app_flex_consumption.validation.identity[0].principal_id
}

resource "azurerm_role_assignment" "validation_func_queue" {
  scope                = azurerm_storage_account.func.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_function_app_flex_consumption.validation.identity[0].principal_id
}

# On the lake: read landing, write archive and quarantine. The archive's WORM policy
# stops it from overwriting or deleting anything already archived.
resource "azurerm_role_assignment" "validation_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_function_app_flex_consumption.validation.identity[0].principal_id
}
