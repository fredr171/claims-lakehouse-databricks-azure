<#
.SYNOPSIS
    Creates the storage account that holds Terraform remote state. Run once, before
    `terraform init`.

.DESCRIPTION
    Terraform cannot create the account that stores its own state, so this script does it
    with the Azure CLI. It lives in its own resource group, so `terraform destroy` of the
    lakehouse never touches it. Safe to run again: every step is idempotent.

    The account allows Entra ID access only (no shared keys). The signed-in user gets
    Storage Blob Data Contributor on it, and the generated backend.hcl tells Terraform
    to authenticate with Entra ID.

    Writes infra/terraform/backend.hcl (git-ignored) for `terraform init -backend-config`.

.EXAMPLE
    ./infra/bootstrap/bootstrap-state.ps1 -Suffix fr42
#>
[CmdletBinding()]
param(
    # Short lowercase suffix that makes storage account names globally unique.
    # Use the same value as `suffix` in terraform.tfvars.
    [Parameter(Mandatory)]
    [ValidatePattern('^[a-z0-9]{3,7}$')]
    [string] $Suffix,

    [string] $Location = 'centralus'
)

$ErrorActionPreference = 'Stop'

$resourceGroup = 'rg-claims-lakehouse-tfstate'
$account = "sttfstateclaims$Suffix"
$container = 'tfstate'
$stateKey = 'claims-lakehouse-dev.tfstate'
$tags = 'project=claims-lakehouse-azure'

function Invoke-Az {
    # Run az and stop on a non-zero exit code (native commands do not throw on their own).
    $output = & az @args
    if ($LASTEXITCODE -ne 0) { throw "az $($args -join ' ') failed with exit code $LASTEXITCODE" }
    $output
}

$subscription = Invoke-Az account show --query name -o tsv
Write-Host "Subscription: $subscription"
$userId = Invoke-Az ad signed-in-user show --query id -o tsv

Write-Host "Resource group $resourceGroup"
Invoke-Az group create --name $resourceGroup --location $Location --tags $tags -o none

Write-Host "Storage account $account"
Invoke-Az storage account create --name $account --resource-group $resourceGroup `
    --location $Location --sku Standard_LRS --kind StorageV2 `
    --min-tls-version TLS1_2 --allow-blob-public-access false `
    --allow-shared-key-access false --tags $tags -o none

# Versioning and soft delete let a corrupted or deleted state file be recovered.
Invoke-Az storage account blob-service-properties update --account-name $account `
    --resource-group $resourceGroup --enable-versioning true `
    --enable-delete-retention true --delete-retention-days 7 -o none

# Created through the management plane, so no data-plane role is needed yet.
Write-Host "Container $container"
Invoke-Az storage container-rm create --storage-account $account `
    --resource-group $resourceGroup --name $container -o none

$accountId = Invoke-Az storage account show --name $account --resource-group $resourceGroup `
    --query id -o tsv
$role = 'Storage Blob Data Contributor'
$existing = Invoke-Az role assignment list --assignee $userId --scope $accountId `
    --role $role --query 'length(@)' -o tsv
if ($existing -eq '0') {
    Write-Host "Granting $role to the signed-in user"
    Invoke-Az role assignment create --assignee-object-id $userId `
        --assignee-principal-type User --role $role --scope $accountId -o none
    Write-Host 'Role assignments can take a few minutes to apply before terraform init works.'
}

$backendFile = Join-Path $PSScriptRoot '..\terraform\backend.hcl'
@"
resource_group_name  = "$resourceGroup"
storage_account_name = "$account"
container_name       = "$container"
key                  = "$stateKey"
use_azuread_auth     = true
"@ | Set-Content -Path $backendFile -Encoding ascii

Write-Host "Wrote $((Resolve-Path $backendFile).Path)"
Write-Host 'Next: cd infra/terraform; terraform init -backend-config=backend.hcl'
