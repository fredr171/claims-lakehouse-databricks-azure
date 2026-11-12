# Unity Catalog objects in the workspace's (automatically assigned) metastore. The
# metastore has no root storage, so the catalog's managed tables live in the lake's
# `unity` container. Jobs and pipelines are code-level resources owned by the bundle
# (databricks.yml), not by Terraform.

# The access connector's managed identity, as Unity Catalog sees it.
resource "databricks_storage_credential" "lake" {
  name    = local.names.storage_credential
  comment = "Lake access through ${local.names.access_connector}"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.main.id
  }

  # Role assignments can take a minute to apply; the credential is validated on use.
  depends_on = [azurerm_role_assignment.access_connector_lake]
}

# The archive is the audit record: Databricks reads it (Auto Loader, the file-arrival
# trigger) and never writes it. Read-only makes that a rule, not a convention.
resource "databricks_external_location" "archive" {
  name            = "archive"
  url             = "abfss://archive@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lake.name
  read_only       = true
  comment         = "Validated deliveries, checksum-keyed (written only by the validation Function)"
  force_destroy   = true

  # The file-arrival trigger reads file events from the queue Terraform provides
  # (eventgrid.tf), so the subscription and the queue role must exist first.
  enable_file_events = true
  file_event_queue {
    provided_aqs {
      queue_url       = azurerm_storage_queue.archive_events.url
      resource_group  = azurerm_resource_group.main.name
      subscription_id = data.azurerm_client_config.current.subscription_id
    }
  }
  depends_on = [
    azurerm_eventgrid_system_topic_event_subscription.archive,
    azurerm_role_assignment.access_connector_archive_queue,
  ]
}

resource "databricks_external_location" "unity" {
  name            = "unity"
  url             = "abfss://unity@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lake.name
  comment         = "Managed storage for the ${local.names.catalog} catalog"
  force_destroy   = true
}

resource "databricks_catalog" "main" {
  name          = local.names.catalog
  storage_root  = "${databricks_external_location.unity.url}${local.names.catalog}"
  comment       = "Claims lakehouse (${var.environment}): bronze, per-payer silver, gold"
  force_destroy = true # the environment is torn down between live runs
}

resource "databricks_schema" "main" {
  for_each = local.schemas

  catalog_name  = databricks_catalog.main.name
  name          = each.key
  comment       = each.value
  force_destroy = true
}

# Answer keys for the live run's final check (uploaded with the generated deliveries).
resource "databricks_volume" "answer_keys" {
  catalog_name = databricks_catalog.main.name
  schema_name  = databricks_schema.main["ops"].name
  name         = "answer_keys"
  volume_type  = "MANAGED"
  comment      = "synthetic-payer-files answer keys for the generated months"
}
