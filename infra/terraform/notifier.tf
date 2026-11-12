# The notifier (functions.tf defines the app): who may call it, and what calls it.
#
#   job's report_outcome task --POST /api/job-outcome--> notifier --> GitHub issues
#   Azure Monitor alert --action group--> POST /api/alert --^
#
# Both endpoints need the Function's default key. The job reads its URL from a Databricks
# secret, and the action group holds its own copy; neither is ever committed.

# The GitHub token is set by hand (README) and read through a Key Vault reference.
resource "azurerm_role_assignment" "notifier_key_vault" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azapi_resource.function["notifier"].identity[0].principal_id
}

data "azapi_resource_action" "notifier_keys" {
  type                   = "Microsoft.Web/sites/host@2025-03-01"
  resource_id            = "${azapi_resource.function["notifier"].id}/host/default"
  action                 = "listkeys"
  method                 = "POST"
  response_export_values = ["functionKeys.default"]
}

locals {
  notifier_host = azapi_resource.function["notifier"].output.properties.defaultHostName
  notifier_key  = data.azapi_resource_action.notifier_keys.output.functionKeys.default
}

# --- Job outcomes ------------------------------------------------------------------------

resource "databricks_secret_scope" "notifier" {
  name = local.names.notifier_secret_scope
}

resource "databricks_secret" "job_outcome_url" {
  scope        = databricks_secret_scope.notifier.name
  key          = "job-outcome-url"
  string_value = "https://${local.notifier_host}/api/job-outcome?code=${local.notifier_key}"
}

# The job runs as the runner, which only needs to read the URL.
resource "databricks_secret_acl" "runner_reads_notifier" {
  scope      = databricks_secret_scope.notifier.name
  principal  = databricks_service_principal.runner.application_id
  permission = "READ"
}

# --- Azure Monitor alerts ----------------------------------------------------------------

resource "azurerm_monitor_action_group" "notifier" {
  name                = local.names.notifier_action_group
  resource_group_name = azurerm_resource_group.main.name
  short_name          = "notifier"
  tags                = local.tags

  webhook_receiver {
    name                    = "notifier"
    service_uri             = "https://${local.notifier_host}/api/alert?code=${local.notifier_key}"
    use_common_alert_schema = true
  }
}

# A quarantined delivery needs a corrected re-send from the payer: one alert per feed.
# Not auto-resolved: a quiet window does not mean the file was replaced, so the issue
# stays open until someone closes it.
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "files_quarantined" {
  name                    = "files-quarantined"
  resource_group_name     = azurerm_resource_group.main.name
  location                = azurerm_resource_group.main.location
  scopes                  = [azurerm_application_insights.main.id]
  description             = "Validation quarantined a delivered file"
  severity                = 3 # warned: other files and feeds carry on
  evaluation_frequency    = "PT15M"
  window_duration         = "PT15M"
  auto_mitigation_enabled = false
  tags                    = local.tags

  criteria {
    query                   = <<-KQL
      traces
      | where message startswith "validation "
      | extend record = parse_json(substring(message, strlen("validation ")))
      | where tostring(record.outcome) == "quarantined"
      | extend feed = tostring(record.feed)
    KQL
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0

    dimension {
      name     = "feed"
      operator = "Include"
      values   = ["*"]
    }
  }

  action {
    action_groups = [azurerm_monitor_action_group.notifier.id]
  }
}

# The validation Function failing (a storage error, a poison message): resolves itself
# once a window passes with no failures.
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "validation_failures" {
  name                    = "validation-function-failures"
  resource_group_name     = azurerm_resource_group.main.name
  location                = azurerm_resource_group.main.location
  scopes                  = [azurerm_application_insights.main.id]
  description             = "The validation Function raised an exception"
  severity                = 1
  evaluation_frequency    = "PT15M"
  window_duration         = "PT15M"
  auto_mitigation_enabled = true
  tags                    = local.tags

  criteria {
    query                   = <<-KQL
      requests
      | where cloud_RoleName =~ "${local.names.validation_function}" and success == false
    KQL
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0
  }

  action {
    action_groups = [azurerm_monitor_action_group.notifier.id]
  }
}
