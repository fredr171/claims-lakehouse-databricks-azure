<#
.SYNOPSIS
    Tears down the dev environment: terraform destroy, then the Databricks managed
    resource group if Azure left it behind.

.DESCRIPTION
    When Unity Catalog is enabled automatically, Azure Databricks adds a workspace catalog
    storage account and a `unity-catalog-access-connector` to the workspace's managed
    resource group. After the workspace is deleted, that group can stay behind, outside
    Terraform. A leftover group would also block the next `terraform apply`, which needs
    the name to be free.

    The group is deleted only if its managedBy still names this environment's workspace
    and that workspace no longer exists. The Terraform state account is never touched.

.EXAMPLE
    ./infra/teardown.ps1
#>
[CmdletBinding()]
param(
    [string] $ResourceGroup = 'rg-claims-lakehouse-dev',
    [string] $ManagedResourceGroup = 'rg-claims-lakehouse-dev-databricks',
    [string] $Workspace = 'dbw-claims-lakehouse-dev'
)

$ErrorActionPreference = 'Stop'

function Invoke-Native {
    & $args[0] @($args | Select-Object -Skip 1)
    if ($LASTEXITCODE -ne 0) { throw "$($args -join ' ') failed with exit code $LASTEXITCODE" }
}

$env:ARM_SUBSCRIPTION_ID = Invoke-Native az account show --query id -o tsv
$workspaceId = "/subscriptions/$env:ARM_SUBSCRIPTION_ID/resourceGroups/$ResourceGroup" +
    "/providers/Microsoft.Databricks/workspaces/$Workspace"

Push-Location (Join-Path $PSScriptRoot 'terraform')
try {
    Invoke-Native terraform destroy -input=false -auto-approve
}
finally {
    Pop-Location
}

if ((Invoke-Native az group exists -n $ManagedResourceGroup) -ne 'true') {
    Write-Host "Done. $ManagedResourceGroup is already gone."
    return
}

$managedBy = Invoke-Native az group show -n $ManagedResourceGroup --query managedBy -o tsv
if ($managedBy -ne $workspaceId) {
    throw "$ManagedResourceGroup is managed by '$managedBy', not $Workspace; leaving it alone."
}
$workspaceLeft = Invoke-Native az resource list --resource-type Microsoft.Databricks/workspaces `
    --query "[?id=='$workspaceId'].id" -o tsv
if ($workspaceLeft) {
    throw "$Workspace still exists; Azure Databricks will remove $ManagedResourceGroup itself."
}

Write-Host "Deleting leftover $ManagedResourceGroup (workspace catalog storage and connector)"
Invoke-Native az group delete -n $ManagedResourceGroup --yes
Write-Host 'Done.'
