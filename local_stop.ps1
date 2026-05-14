$ErrorActionPreference = 'Stop'

$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PgDir = if ($env:CAPUT_PG_DIR) { $env:CAPUT_PG_DIR } else { 'C:\Users\home\Desktop\caput-local-postgres' }
$PgData = Join-Path $PgDir 'data'
$PidFile = Join-Path $RepoDir 'caput_app.pid'

function Stop-ProcessTree {
    param([int]$RootPid)

    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $RootPid" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree -RootPid $child.ProcessId
    }

    $process = Get-Process -Id $RootPid -ErrorAction SilentlyContinue
    if ($process) {
        Stop-Process -Id $RootPid -Force -ErrorAction SilentlyContinue
    }
}

if (Test-Path $PidFile) {
    $pidValue = Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($pidValue) {
        $process = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
        if ($process) {
            Write-Host "Stopping Caput app PID $pidValue"
            Stop-ProcessTree -RootPid ([int]$pidValue)
        }
    }
    Remove-Item -LiteralPath $PidFile -Force
}

if (Test-Path $PgData) {
    $pgStatus = & pg_ctl -D $PgData status 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Stopping local Postgres"
        & pg_ctl -D $PgData stop -m fast
    } else {
        Write-Host "Local Postgres is already stopped"
    }
}
