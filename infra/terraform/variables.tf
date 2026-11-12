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

variable "monthly_budget" {
  description = "Monthly cost budget for the environment, in the billing currency."
  type        = number
  default     = 25
}

variable "budget_alert_emails" {
  description = "Who the budget emails at 50% and 90% of actual spend and 100% forecast. Set it in terraform.tfvars, never in the repo."
  type        = list(string)

  validation {
    condition     = length(var.budget_alert_emails) > 0
    error_message = "budget_alert_emails needs at least one address."
  }
}
