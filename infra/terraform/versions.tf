terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
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

    # Application Insights otherwise creates a "Failure Anomalies" alert rule outside
    # Terraform, which then blocks deleting the resource group.
    application_insights {
      disable_generated_rule = true
    }
  }

  # Storage accounts here disallow shared keys, so data-plane calls use Entra ID.
  storage_use_azuread = true
}
