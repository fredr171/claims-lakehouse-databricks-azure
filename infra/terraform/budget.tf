# A monthly cost budget that emails when the environment's spend gets close to the limit.
#
# Scoped to the subscription but filtered to this environment's two resource groups: ours
# and the Databricks managed one, which carries the workspace's own storage. Databricks
# serverless is billed on the workspace, so it counts too. The budget lives and dies with the
# environment, which is exactly when it can spend; Cost Management lags by up to a day.

data "azurerm_subscription" "current" {}

resource "azurerm_consumption_budget_subscription" "main" {
  name            = "budget-${local.project}-${var.environment}"
  subscription_id = data.azurerm_subscription.current.id
  amount          = var.monthly_budget
  time_grain      = "Monthly"

  time_period {
    # The current month; the API needs the first of a month and the budget keeps running.
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  filter {
    dimension {
      name   = "ResourceGroupName"
      values = [local.names.resource_group, local.names.databricks_managed]
    }
  }

  dynamic "notification" {
    for_each = {
      actual_50    = { threshold = 50, type = "Actual" }
      actual_90    = { threshold = 90, type = "Actual" }
      forecast_100 = { threshold = 100, type = "Forecasted" }
    }
    content {
      enabled        = true
      threshold      = notification.value.threshold
      threshold_type = notification.value.type
      operator       = "GreaterThan"
      contact_emails = var.budget_alert_emails
    }
  }

  lifecycle {
    # start_date is fixed at creation; a later plan would otherwise replace the budget.
    ignore_changes = [time_period]
  }
}
