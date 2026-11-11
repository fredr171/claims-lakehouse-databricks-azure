output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "lake_storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "lake_dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}

output "validation_function_name" {
  value = azurerm_function_app_flex_consumption.validation.name
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "databricks_workspace_url" {
  description = "Put this in DATABRICKS_HOST (.env, git-ignored)."
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_access_connector_id" {
  value = azurerm_databricks_access_connector.main.id
}
