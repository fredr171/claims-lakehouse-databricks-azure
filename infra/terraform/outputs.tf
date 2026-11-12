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
  value = azapi_resource.function["validation"].name
}

output "notifier_function_name" {
  value = azapi_resource.function["notifier"].name
}

output "github_token_secret" {
  description = "Key Vault secret to hold the notifier's GitHub token (set by hand)."
  value       = local.github_token_secret
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

output "runner_application_id" {
  description = "The service principal the pipelines and the job run as (bundle run_as)."
  value       = databricks_service_principal.runner.application_id
}

output "pii_readers_group" {
  description = "Members see unmasked names, addresses and birth dates."
  value       = databricks_group.pii_readers.display_name
}

output "notifier_secret_scope" {
  description = "Databricks secret scope with the notifier's job-outcome URL (bundle variable)."
  value       = databricks_secret_scope.notifier.name
}

# GitHub environment variables for the deploy workflow (Settings -> Environments -> dev).
output "github_deploy_client_id" {
  description = "AZURE_CLIENT_ID for the deploy workflow."
  value       = azuread_application.github_deploy.client_id
}
