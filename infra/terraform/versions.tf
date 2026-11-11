terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    azapi = {
      source  = "azure/azapi"
      version = "~> 2.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }

  # Partial configuration: the values come from backend.hcl, which
  # infra/bootstrap/bootstrap-state.ps1 writes. See backend.hcl.example.
  backend "azurerm" {}
}

provider "azurerm" {
  # The subscription comes from ARM_SUBSCRIPTION_ID, never from the repo.
  features {
    # The environment is torn down between live runs, and a vault name stays reserved
    # while it is soft-deleted, so purge it on destroy.
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }

    # Application Insights creates a "Smart Detection" action group outside Terraform,
    # which would otherwise block deleting the resource group on destroy. Everything in
    # the group belongs to this environment, so let destroy remove it.
    # (The provider's application_insights.disable_generated_rule waits for an alert rule
    # that Azure no longer creates, and hangs the apply until its timeout.)
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }

  # Storage accounts here disallow shared keys, so data-plane calls use Entra ID.
  storage_use_azuread = true
}

# Only for resources azurerm cannot express correctly (see functions.tf). Uses the same
# Azure CLI sign-in and ARM_SUBSCRIPTION_ID as azurerm.
provider "azapi" {}

# Workspace-level Unity Catalog objects (unity_catalog.tf), as the Azure CLI user, who is
# workspace admin. The workspace is created in this same configuration.
provider "databricks" {
  host                        = "https://${azurerm_databricks_workspace.main.workspace_url}"
  azure_workspace_resource_id = azurerm_databricks_workspace.main.id
  auth_type                   = "azure-cli"
}
