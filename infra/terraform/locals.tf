locals {
  project = "claims-lakehouse"

  # Every resource name is defined here, once.
  names = {
    resource_group = "rg-${local.project}-${var.environment}"
    lake_storage   = "stclaimslakehouse${var.suffix}"
  }

  # Containers in the lake account, in flow order (see the README).
  lake_containers = ["landing", "archive", "quarantine"]

  tags = {
    project     = "claims-lakehouse-azure"
    environment = var.environment
  }
}
