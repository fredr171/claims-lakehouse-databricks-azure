# Logs for the Functions. A daily cap keeps a runaway log loop inside the budget.
resource "azurerm_log_analytics_workspace" "main" {
  name                = local.names.log_analytics
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  daily_quota_gb      = 0.5
  tags                = local.tags
}

resource "azurerm_application_insights" "main" {
  name                = local.names.app_insights
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "other"
  tags                = local.tags
}
