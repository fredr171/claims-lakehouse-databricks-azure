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
  features {}

  # Storage accounts here disallow shared keys, so data-plane calls use Entra ID.
  storage_use_azuread = true
}
