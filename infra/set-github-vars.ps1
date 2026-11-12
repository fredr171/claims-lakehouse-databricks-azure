<#
.SYNOPSIS
    Sets the GitHub `dev` environment variables the deploy workflow reads, from Terraform.

.DESCRIPTION
    Run after `terraform apply`. A fresh environment has a new deploy app, workspace and
    runner, so their IDs change on every apply; this keeps the variables in step without
    copying them by hand. None of the values are secrets, and none are written to the repo.

.EXAMPLE
    ./infra/set-github-vars.ps1
#>
[CmdletBinding()]
param([string] $Environment = 'dev')

$ErrorActionPreference = 'Stop'

function Invoke-Native {
    $out = & $args[0] @($args | Select-Object -Skip 1)
    if ($LASTEXITCODE -ne 0) { throw "$($args[0]) $($args[1]) failed with exit code $LASTEXITCODE" }
    $out
}

$tf = Join-Path $PSScriptRoot 'terraform'
function Get-Output([string] $name) { Invoke-Native terraform "-chdir=$tf" output -raw $name }

$account = Invoke-Native az account show -o json | ConvertFrom-Json
$variables = [ordered]@{
    AZURE_CLIENT_ID       = Get-Output 'github_deploy_client_id'
    AZURE_TENANT_ID       = $account.tenantId
    AZURE_SUBSCRIPTION_ID = $account.id
    RESOURCE_GROUP        = Get-Output 'resource_group_name'
    VALIDATION_FUNCTION   = Get-Output 'validation_function_name'
    NOTIFIER_FUNCTION     = Get-Output 'notifier_function_name'
    DATABRICKS_HOST       = Get-Output 'databricks_workspace_url'
    ARCHIVE_URL           = Get-Output 'archive_url'
    RUNNER_APPLICATION_ID = Get-Output 'runner_application_id'
}

foreach ($name in $variables.Keys) {
    Invoke-Native gh variable set $name --env $Environment --body $variables[$name] | Out-Null
    Write-Host "set $name"
}
Write-Host "Done. Run the deploy with: gh workflow run deploy.yml"
