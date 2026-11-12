# The Functions, both Flex Consumption (FC1), which bills per execution, scales to zero
# and supports managed identity for its own storage, so no storage keys are needed:
#   validation  checks every file that lands, then copies it to archive or quarantine
#   notifier    turns job outcomes and Azure Monitor alerts into GitHub issues (notifier.tf)
# Flex Consumption allows one app per plan, so each Function has its own plan.

locals {
  functions = {
    validation = {
      name = local.names.validation_function
      settings = [
        { name = "LANDING_EVENTS_QUEUE", value = local.landing_events_queue },
        { name = "LAKE_BLOB_ENDPOINT", value = azurerm_storage_account.lake.primary_blob_endpoint },
        { name = "LANDING_CONTAINER", value = "landing" },
        { name = "ARCHIVE_CONTAINER", value = "archive" },
        { name = "QUARANTINE_CONTAINER", value = "quarantine" },
      ]
    }
    notifier = {
      name = local.names.notifier_function
      settings = [
        { name = "GITHUB_REPOSITORY", value = var.github_repository },
        # A Key Vault reference: the platform reads the secret with the app's identity.
        {
          name  = "GITHUB_TOKEN"
          value = "@Microsoft.KeyVault(VaultName=${azurerm_key_vault.main.name};SecretName=${local.github_token_secret})"
        },
      ]
    }
  }
}

# The Functions' own storage: deployment packages, host state, the landing-events queue
# and the Event Grid dead-letter container. Separate from the lake so runtime files never
# mix with data.
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
  for_each = local.functions

  name                  = "${local.func_deploy_container}-${each.key}"
  storage_account_id    = azurerm_storage_account.func.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "event_deadletter" {
  name                  = local.event_deadletter_container
  storage_account_id    = azurerm_storage_account.func.id
  container_access_type = "private"
}

# Event Grid delivers each landing event here; the validation Function's queue trigger
# reads it.
resource "azurerm_storage_queue" "landing_events" {
  name               = local.landing_events_queue
  storage_account_id = azurerm_storage_account.func.id
}

resource "azurerm_service_plan" "functions" {
  for_each = local.functions

  name                = "${local.names.function_plan}-${each.key}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = local.tags
}

# Defined with azapi rather than azurerm_function_app_flex_consumption, which always
# injects a key-based AzureWebJobsStorage connection string (hashicorp/terraform-provider-azurerm#29149).
# This storage account rejects keys, so the host could not reach its queue. The body
# follows Microsoft's Flex Consumption managed-identity sample.
resource "azapi_resource" "function" {
  for_each = local.functions

  type      = "Microsoft.Web/sites@2025-03-01"
  name      = each.value.name
  parent_id = azurerm_resource_group.main.id
  location  = azurerm_resource_group.main.location
  tags      = local.tags

  identity {
    type         = "SystemAssigned"
    identity_ids = []
  }

  body = {
    kind = "functionapp,linux"
    properties = {
      # Azure stores the ID with lowercase `serverfarms`; match it so plans stay clean.
      serverFarmId = replace(azurerm_service_plan.functions[each.key].id, "Microsoft.Web/serverFarms", "Microsoft.Web/serverfarms")
      httpsOnly    = true
      functionAppConfig = {
        deployment = {
          storage = {
            type           = "blobContainer"
            value          = "${azurerm_storage_account.func.primary_blob_endpoint}${azurerm_storage_container.func_deploy[each.key].name}"
            authentication = { type = "SystemAssignedIdentity" }
          }
        }
        scaleAndConcurrency = {
          maximumInstanceCount = 40
          instanceMemoryMB     = 2048
        }
        # Python 3.12 matches the repo's local environment and Databricks serverless.
        runtime = { name = "python", version = "3.12" }
      }
      siteConfig = {
        minTlsVersion = "1.2"
        appSettings = concat([
          # Identity-based connection to the Functions' storage (queue trigger, host
          # state). There is deliberately no AzureWebJobsStorage connection string.
          { name = "AzureWebJobsStorage__accountName", value = azurerm_storage_account.func.name },
          { name = "APPLICATIONINSIGHTS_CONNECTION_STRING", value = azurerm_application_insights.main.connection_string },
        ], each.value.settings)
      }
    }
  }

  response_export_values = ["properties.defaultHostName"]
}

# Each Function's identity on the Functions' storage: deployment package and host state.
resource "azurerm_role_assignment" "func_blob" {
  for_each = local.functions

  scope                = azurerm_storage_account.func.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azapi_resource.function[each.key].identity[0].principal_id
}

# The validation queue trigger: receive, delete, poison queue.
resource "azurerm_role_assignment" "validation_func_queue" {
  scope                = azurerm_storage_account.func.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azapi_resource.function["validation"].identity[0].principal_id
}

# On the lake: read landing, write archive and quarantine. The archive's WORM policy
# stops it from overwriting or deleting anything already archived.
resource "azurerm_role_assignment" "validation_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azapi_resource.function["validation"].identity[0].principal_id
}
