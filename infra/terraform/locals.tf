locals {
  project = "claims-lakehouse"

  # Every resource name is defined here, once. Globally unique names carry the suffix.
  names = {
    resource_group        = "rg-${local.project}-${var.environment}"
    lake_storage          = "stclaimslakehouse${var.suffix}"
    func_storage          = "stclaimsfunc${var.suffix}"
    log_analytics         = "log-${local.project}-${var.environment}"
    app_insights          = "appi-${local.project}-${var.environment}"
    function_plan         = "asp-${local.project}" # + -<function>-<environment>
    validation_function   = "func-${local.project}-validation-${var.suffix}"
    notifier_function     = "func-${local.project}-notifier-${var.suffix}"
    event_grid_topic      = "evgt-${local.project}-lake-${var.environment}"
    key_vault             = "kv-claimslake-${var.suffix}"
    databricks            = "dbw-${local.project}-${var.environment}"
    databricks_managed    = "rg-${local.project}-${var.environment}-databricks"
    access_connector      = "dbac-${local.project}-${var.environment}"
    storage_credential    = "claims_lakehouse_lake_${var.environment}"
    catalog               = "claims_lakehouse_${var.environment}"
    runner                = "${local.project}-runner-${var.environment}"
    pii_readers           = "${local.project}-pii-readers-${var.environment}"
    notifier_secret_scope = "${local.project}-notifier"
    notifier_action_group = "ag-${local.project}-notifier-${var.environment}"
  }

  # Unity Catalog privileges of the runner (runner.tf).
  runner_privileges = {
    data = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE", "CREATE_MATERIALIZED_VIEW"]
    ops  = ["USE_SCHEMA", "CREATE_FUNCTION", "EXECUTE", "READ_VOLUME"]
  }

  # Containers in the lake account, in flow order (see the README). `unity` holds the
  # catalog's managed tables.
  lake_containers = ["landing", "archive", "quarantine", "unity"]

  # Schemas in the catalog. One silver schema per payer feed, so each payer's pipeline
  # publishes into its own schema.
  schemas = {
    bronze        = "Every validated delivery, all columns as strings, plus lineage"
    silver_cclf   = "CMS CCLF (Medicare FFS) silver, written by the CCLF pipeline"
    silver_humana = "Humana Medicare Advantage silver, written by the Humana pipeline"
    gold          = "Payer-agnostic marts"
    ops           = "Answer keys and run bookkeeping"
  }

  # In the Function's own storage account.
  func_deploy_container      = "app-package"
  landing_events_queue       = "landing-events"
  event_deadletter_container = "eventgrid-deadletter"

  # The notifier's GitHub token in Key Vault (set by hand; see the README).
  github_token_secret = "github-token"

  # In the lake account: archive events for Databricks file events.
  archive_events_queue = "archive-events"

  tags = {
    project     = "claims-lakehouse-azure"
    environment = var.environment
  }
}
