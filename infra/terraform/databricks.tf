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

# File events: Databricks reads (and deletes) messages from the archive queue that
# Terraform provides (eventgrid.tf). Scoped to that one queue.
resource "azurerm_role_assignment" "access_connector_archive_queue" {
  scope                = azurerm_storage_queue.archive_events.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}
