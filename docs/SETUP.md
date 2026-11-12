# Setup

## Local

Requires Python 3.12 and Java 17 (for local Spark). From PowerShell:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\ruff check .
.venv\Scripts\pytest -m "not slow"   # fast subset, as in CI
.venv\Scripts\pytest                 # everything, including the 36-month answer-key tests
```

Set `JAVA_HOME` to the JDK 17 folder if `java` is not on `PATH`.

## Infrastructure

Requires Terraform 1.9+ and the Azure CLI, signed in with `az login`. Choose a suffix of 3–7
lowercase letters or digits; it makes the storage and Key Vault names globally unique.

```powershell
./infra/bootstrap/bootstrap-state.ps1 -Suffix <suffix>   # once: remote-state account
Copy-Item infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars  # set suffix and budget emails
$env:ARM_SUBSCRIPTION_ID = az account show --query id -o tsv
cd infra/terraform
terraform init "-backend-config=backend.hcl"
terraform plan
terraform apply      # creates billable resources
cd ../..; ./infra/teardown.ps1   # between live runs: destroy, plus the Databricks leftovers
```

Deploy both Functions after `apply` (dependencies install in a remote build):

```powershell
foreach ($app in 'validation', 'notifier') {
    ./functions/build.ps1 -App $app
    $fn = terraform -chdir=infra/terraform output -raw "${app}_function_name"
    az functionapp deployment source config-zip -g rg-claims-lakehouse-dev -n $fn `
        --src "build/$app.zip" --build-remote true
}
```

The notifier needs a GitHub token in Key Vault: a fine-grained personal access token for
this repository only, with read and write access to issues. Paste it when prompted, so it
never appears in the shell history:

```powershell
$kv = terraform -chdir=infra/terraform output -raw key_vault_name
$name = terraform -chdir=infra/terraform output -raw github_token_secret
$token = Read-Host -AsSecureString 'GitHub token'
az keyvault secret set --vault-name $kv -n $name --value ([pscredential]::new('x', $token).GetNetworkCredential().Password) -o none
```

## Deploying from GitHub

After `terraform apply`, the `deploy` workflow can replace the manual Function and bundle
deploys. Create a `dev` environment in the repository settings, then set its variables
from Terraform with `./infra/set-github-vars.ps1` (none are secrets; none are committed).
Run it after every apply: a fresh environment has a new deploy app, workspace and runner.
The variables it sets:

| Variable | Value |
|---|---|
| `AZURE_CLIENT_ID` | `terraform output -raw github_deploy_client_id` |
| `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID` | `az account show --query "[tenantId, id]"` |
| `RESOURCE_GROUP` | `terraform output -raw resource_group_name` |
| `VALIDATION_FUNCTION`, `NOTIFIER_FUNCTION` | `terraform output -raw validation_function_name` / `notifier_function_name` |
| `DATABRICKS_HOST` | `terraform output -raw databricks_workspace_url` |
| `ARCHIVE_URL`, `RUNNER_APPLICATION_ID` | `terraform output -raw archive_url` / `runner_application_id` |

Then run it with `gh workflow run deploy`.

## Databricks

Requires the Databricks CLI (`winget install Databricks.DatabricksCLI`). It signs in with
the Azure CLI login, and takes the workspace and archive from the environment:

```powershell
$env:DATABRICKS_HOST = terraform -chdir=infra/terraform output -raw databricks_workspace_url
$env:BUNDLE_VAR_archive_url = terraform -chdir=infra/terraform output -raw archive_url
$env:BUNDLE_VAR_runner_application_id = terraform -chdir=infra/terraform output -raw runner_application_id
databricks bundle validate
databricks bundle deploy   # creates the pipelines and the job (billable when they run)
```
