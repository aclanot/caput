$ErrorActionPreference = 'Stop'

$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackupDir = Join-Path $RepoDir 'backup'
$Stamp = Get-Date -AsUTC -Format 'yyyyMMdd_HHmmss'
$Output = Join-Path $BackupDir "caput_local_$Stamp.dump"
$DatabaseUrl = if ($env:CAPUT_DATABASE_URL) { $env:CAPUT_DATABASE_URL } else { 'postgresql://caput@localhost:55432/caput' }

New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
Write-Host "Writing $Output"
& pg_dump --format=custom --compress=9 --no-owner --no-privileges $DatabaseUrl -f $Output
if ($LASTEXITCODE -ne 0) {
    throw "pg_dump failed"
}

Get-FileHash $Output -Algorithm SHA256
