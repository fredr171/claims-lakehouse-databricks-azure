variable "suffix" {
  description = "Short lowercase suffix that makes globally unique names (storage accounts, Key Vault) unique. Same value as the bootstrap script's -Suffix."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,7}$", var.suffix))
    error_message = "suffix must be 3 to 7 lowercase letters or digits."
  }
}

variable "environment" {
  description = "Environment name used in resource names."
  type        = string
  default     = "dev"
}

variable "archive_retention_days" {
  description = "Days an archived delivery stays immutable (container-level WORM on the archive)."
  type        = number
  default     = 365
}

variable "location" {
  description = "Azure region for every resource."
  type        = string
  default     = "centralus"
}

variable "github_repository" {
  description = "owner/name of the repository the notifier opens issues in."
  type        = string
  default     = "fredr171/claims-lakehouse-databricks-azure"
}
