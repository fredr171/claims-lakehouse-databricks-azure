# The lake: ADLS Gen2 (hierarchical namespace), which Unity Catalog external locations
# require. Entra ID access only; the Functions and Databricks use managed identities.
resource "azurerm_storage_account" "lake" {
  name                     = local.names.lake_storage
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  default_to_oauth_authentication = true

  tags = local.tags
}

resource "azurerm_storage_container" "lake" {
  for_each = toset(local.lake_containers)

  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# Lets the person running Terraform upload test deliveries to landing and inspect the
# other containers.
resource "azurerm_role_assignment" "deployer_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
