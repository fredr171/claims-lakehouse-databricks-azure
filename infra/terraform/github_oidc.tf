# The identity the deploy workflow (.github/workflows/deploy.yml) signs in as, with OIDC:
# GitHub issues a short-lived token for a run in this repository's `dev` environment, and
# Entra ID trusts it through a federated credential. No Azure secret is stored in GitHub.
#
# It deploys code only (both Functions and the Databricks bundle). Infrastructure changes
# stay a reviewed, local `terraform apply`, so it gets no role on the resource group.

data "azuread_client_config" "current" {}

resource "azuread_application" "github_deploy" {
  display_name = local.names.github_deploy
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "github_deploy" {
  client_id = azuread_application.github_deploy.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

resource "azuread_application_federated_identity_credential" "github_dev" {
  application_id = azuread_application.github_deploy.id
  display_name   = "github-${var.environment}"
  description    = "Deploy workflow runs in the ${var.environment} environment"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  subject        = "repo:${var.github_repository}:environment:${var.environment}"
}

# Zip deploy to each Function app, nothing else in Azure.
resource "azurerm_role_assignment" "github_deploy_functions" {
  for_each = local.functions

  scope                = azapi_resource.function[each.key].id
  role_definition_name = "Website Contributor"
  principal_id         = azuread_service_principal.github_deploy.object_id
}

# In Databricks it deploys the bundle (jobs and pipelines that run as the runner).
resource "databricks_service_principal" "github_deploy" {
  application_id   = azuread_application.github_deploy.client_id
  display_name     = local.names.github_deploy
  workspace_access = true
}
