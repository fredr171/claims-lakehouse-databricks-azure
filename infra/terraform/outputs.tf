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
  value = azapi_resource.validation_function.name
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

# Bundle variables (databricks.yml): pass with --var or a git-ignored variable file.
output "catalog" {
  value = databricks_catalog.main.name
}

output "archive_url" {
  description = "Auto Loader source and the job's file-arrival trigger."
  value       = databricks_external_location.archive.url
}
