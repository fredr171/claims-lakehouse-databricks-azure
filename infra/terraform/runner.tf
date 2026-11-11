# The identity the pipelines and the job run as (the bundle's run_as), and what it may do.
#
# Column masks hide members' names, addresses and birth dates from everyone outside the PII
# readers group. The runner is in the group, so silver and gold are computed from real
# values; people querying the tables are not, so they see masked values. Production would
# use an account-level group managed in Entra ID.

resource "databricks_service_principal" "runner" {
  display_name = local.names.runner
}

resource "databricks_group" "pii_readers" {
  display_name = local.names.pii_readers
}

resource "databricks_group_member" "runner_reads_pii" {
  group_id  = databricks_group.pii_readers.id
  member_id = databricks_service_principal.runner.id
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.main.name

  grant {
    principal  = databricks_service_principal.runner.application_id
    privileges = ["USE_CATALOG"]
  }
}

# Data schemas: the runner creates and refreshes the tables. ops: the mask functions and
# the answer keys.
resource "databricks_grants" "schemas" {
  for_each = local.schemas

  schema = "${databricks_catalog.main.name}.${databricks_schema.main[each.key].name}"

  grant {
    principal  = databricks_service_principal.runner.application_id
    privileges = each.key == "ops" ? local.runner_privileges.ops : local.runner_privileges.data
  }
}

# Auto Loader and the file-arrival trigger read the archive; nothing writes it.
resource "databricks_grants" "archive" {
  external_location = databricks_external_location.archive.id

  grant {
    principal  = databricks_service_principal.runner.application_id
    privileges = ["READ_FILES"]
  }
}

# Whoever runs Terraform (and deploys the bundle) may set the runner as a job's or a
# pipeline's run_as: that needs the Service Principal User role on it, which creating the
# service principal does not grant.
# The account ID comes from the workspace's public discovery document, so none is committed.
data "http" "workspace_config" {
  url = "https://${azurerm_databricks_workspace.main.workspace_url}/.well-known/databricks-config"
}

data "databricks_current_user" "deployer" {}

resource "databricks_access_control_rule_set" "runner_users" {
  name = "accounts/${jsondecode(data.http.workspace_config.response_body).account_id}/servicePrincipals/${databricks_service_principal.runner.application_id}/ruleSets/default"

  grant_rules {
    principals = [data.databricks_current_user.deployer.acl_principal_id]
    role       = "roles/servicePrincipal.user"
  }
}