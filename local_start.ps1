$ErrorActionPreference = 'Stop'

$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PgDir = if ($env:CAPUT_PG_DIR) { $env:CAPUT_PG_DIR } else { 'C:\Users\home\Desktop\caput-local-postgres' }
$PgData = Join-Path $PgDir 'data'
$PgLog = Join-Path $PgDir 'postgres.log'
$AppOutLog = Join-Path $RepoDir 'caput_app.out.log'
$AppErrLog = Join-Path $RepoDir 'caput_app.err.log'
$PidFile = Join-Path $RepoDir 'caput_app.pid'
$PythonExe = Join-Path $RepoDir '.venv\Scripts\python.exe'
$Port = if ($env:CAPUT_PG_PORT) { $env:CAPUT_PG_PORT } else { '55432' }

if (-not (Test-Path $PgData)) {
    throw "Postgres data directory not found: $PgData"
}

$pgStatus = & pg_ctl -D $PgData status 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Starting local Postgres on port $Port"
    & pg_ctl -D $PgData -l $PgLog start
} else {
    Write-Host "Local Postgres already running"
}

Push-Location $RepoDir
try {
    $existingPid = if (Test-Path $PidFile) { Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
    if ($existingPid) {
        $existingProcess = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
        if ($existingProcess) {
            Write-Host "Caput app already running with PID $existingPid"
            return
        }
    }

    Write-Host "Starting Caput app"
    if (-not (Test-Path $PythonExe)) {
        $PythonExe = 'python'
    }
    $process = Start-Process -FilePath $PythonExe -ArgumentList 'app.py' -WorkingDirectory $RepoDir -RedirectStandardOutput $AppOutLog -RedirectStandardError $AppErrLog -WindowStyle Hidden -PassThru
    Set-Content -LiteralPath $PidFile -Value $process.Id -Encoding ASCII
    Write-Host "Caput app PID $($process.Id). Logs: $AppOutLog and $AppErrLog"
} finally {
    Pop-Location
}
