# Secrets the platform needs at runtime, starting with the notifier's GitHub credential
# (phase 4). Access is by Azure RBAC, not vault access policies.
resource "azurerm_key_vault" "main" {
  name                = local.names.key_vault
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  rbac_authorization_enabled = true

  # Purge protection would keep the vault name reserved for up to 90 days after
  # `terraform destroy`; the dev environment is rebuilt often, so leave it off.
  purge_protection_enabled   = false
  soft_delete_retention_days = 7

  tags = local.tags
}

# Lets the person running Terraform put the GitHub credential into the vault.
resource "azurerm_role_assignment" "deployer_key_vault" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}
