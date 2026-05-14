param(
    [Parameter(Mandatory = $true)]
    [string]$DumpPath
)

$ErrorActionPreference = 'Stop'

$DatabaseUrl = if ($env:CAPUT_DATABASE_URL) { $env:CAPUT_DATABASE_URL } else { 'postgresql://caput@localhost:55432/caput' }

if (-not (Test-Path $DumpPath)) {
    throw "Dump not found: $DumpPath"
}

Write-Host "Restoring $DumpPath into local Caput database"
& pg_restore --clean --if-exists --no-owner --no-privileges --jobs=4 -d $DatabaseUrl $DumpPath
if ($LASTEXITCODE -ne 0) {
    throw "pg_restore failed"
}
