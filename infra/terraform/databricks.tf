# Premium is required for Unity Catalog column masks.
resource "azurerm_databricks_workspace" "main" {
  name                        = local.names.databricks
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"
  managed_resource_group_name = local.names.databricks_managed

  # Jobs, pipelines and SQL run on serverless compute, which does not use the workspace
  # network. Secure cluster connectivity on the default managed network would add a NAT
  # gateway (about $30 a month, more than the whole budget) for classic clusters that are
  # rarely started. Classic clusters therefore get public IPs, still with no inbound
  # ports open. A production workspace would use VNet injection and a NAT gateway.
  custom_parameters {
    no_public_ip = false
  }

  tags = local.tags
}

# The identity Unity Catalog uses to reach the lake (storage credential, phase 3).
resource "azurerm_databricks_access_connector" "main" {
  name                = local.names.access_connector
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# Read the archive (Auto Loader) and write managed tables.
resource "azurerm_role_assignment" "access_connector_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}

# File events: Unity Catalog turns them on for new external locations, and the job's
# file-arrival trigger then reads them instead of listing the archive. Databricks creates
# the Event Grid subscription and storage queue itself, which needs these roles (without
# them every trigger check fails with a 403 on queue.create). Storage Account Contributor
# is a broad management role on the lake account (it could re-enable shared-key access,
# which `terraform plan` would show as drift); production would scope file events to a
# dedicated account or supply its own queue.
resource "azurerm_role_assignment" "access_connector_file_events" {
  for_each = {
    "Storage Account Contributor"             = azurerm_storage_account.lake.id
    "Storage Queue Data Contributor"          = azurerm_storage_account.lake.id
    "EventGrid EventSubscription Contributor" = azurerm_resource_group.main.id
  }

  scope                = each.value
  role_definition_name = each.key
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}
