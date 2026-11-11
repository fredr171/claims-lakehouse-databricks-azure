locals {
  project = "claims-lakehouse"

  # Every resource name is defined here, once. Globally unique names carry the suffix.
  names = {
    resource_group      = "rg-${local.project}-${var.environment}"
    lake_storage        = "stclaimslakehouse${var.suffix}"
    func_storage        = "stclaimsfunc${var.suffix}"
    log_analytics       = "log-${local.project}-${var.environment}"
    app_insights        = "appi-${local.project}-${var.environment}"
    function_plan       = "asp-${local.project}-validation-${var.environment}"
    validation_function = "func-${local.project}-validation-${var.suffix}"
    event_grid_topic    = "evgt-${local.project}-lake-${var.environment}"
    key_vault           = "kv-claimslake-${var.suffix}"
    databricks          = "dbw-${local.project}-${var.environment}"
    databricks_managed  = "rg-${local.project}-${var.environment}-databricks"
    access_connector    = "dbac-${local.project}-${var.environment}"
  }

  # Containers in the lake account, in flow order (see the README).
  lake_containers = ["landing", "archive", "quarantine"]

  # In the Function's own storage account.
  func_deploy_container      = "app-package"
  landing_events_queue       = "landing-events"
  event_deadletter_container = "eventgrid-deadletter"

  tags = {
    project     = "claims-lakehouse-azure"
    environment = var.environment
  }
}
