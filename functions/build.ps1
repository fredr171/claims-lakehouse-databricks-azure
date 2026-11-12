<#
.SYNOPSIS
    Packages one of the Functions (validation or notifier) into a zip for deployment.

.DESCRIPTION
    Copies the Function's files (functions/<App>/), the claims_lakehouse package and the
    config/ folder into build/<App>/, then zips them. Dependencies in requirements.txt are
    installed by the remote build on deploy, so the zip holds only source.

.EXAMPLE
    ./functions/build.ps1 -App validation
    az functionapp deployment source config-zip -g rg-claims-lakehouse-dev `
        -n <function name> --src build/validation.zip --build-remote true
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('validation', 'notifier')]
    [string] $App
)

$ErrorActionPreference = 'Stop'

$repo = Resolve-Path (Join-Path $PSScriptRoot '..')
$source = Join-Path $PSScriptRoot $App
$buildRoot = Join-Path $repo 'build'
$staging = Join-Path $buildRoot $App
$zip = Join-Path $buildRoot "$App.zip"

if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
if (Test-Path $zip) { Remove-Item -Force $zip }
New-Item -ItemType Directory -Force $staging | Out-Null

Get-ChildItem $source -File | Copy-Item -Destination $staging
Copy-Item -Recurse (Join-Path $repo 'src\claims_lakehouse') $staging
Copy-Item -Recurse (Join-Path $repo 'config') $staging

# No caches or compiled files in the package.
Get-ChildItem $staging -Recurse -Directory -Filter '__pycache__' | Remove-Item -Recurse -Force

# Not Compress-Archive: on Windows PowerShell 5.1 it writes entry names with backslashes,
# which the Linux Functions host extracts as literal file names.
Add-Type -AssemblyName System.IO.Compression, System.IO.Compression.FileSystem
$archive = [System.IO.Compression.ZipFile]::Open($zip, 'Create')
try {
    $prefix = (Resolve-Path $staging).Path.TrimEnd('\') + '\'
    foreach ($file in Get-ChildItem $staging -Recurse -File) {
        $entry = $file.FullName.Substring($prefix.Length).Replace('\', '/')
        [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($archive, $file.FullName, $entry)
    }
}
finally {
    $archive.Dispose()
}
Write-Host "Wrote $zip"
