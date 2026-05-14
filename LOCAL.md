# Local Caput Runbook

This setup runs the bot and PostgreSQL on this Windows PC.

## Paths

- Project: `C:\Users\home\Desktop\caput-paper-accounting`
- Local PostgreSQL data: `C:\Users\home\Desktop\caput-local-postgres\data`
- Local database URL: `postgresql://caput@localhost:55432/caput`
- App logs: `caput_app.out.log` and `caput_app.err.log`

## Start

```powershell
powershell -ExecutionPolicy Bypass -File .\local_start.ps1
```

This starts local PostgreSQL, then starts `python app.py` from `.venv`.

## Stop

```powershell
powershell -ExecutionPolicy Bypass -File .\local_stop.ps1
```

This stops the Caput app process tree, then stops the local PostgreSQL cluster.

## Backup

```powershell
powershell -ExecutionPolicy Bypass -File .\local_backup.ps1
```

Backups are written to `backup\` as PostgreSQL custom-format dumps.

## Restore

```powershell
powershell -ExecutionPolicy Bypass -File .\local_restore.ps1 -DumpPath backup\caput_pg_full_20260514_185455.dump
```

Restore is destructive for matching database objects because it uses `--clean --if-exists`.

## Health Checks

```powershell
psql -h localhost -p 55432 -U caput -d caput -c "select max(ts), count(*) from token_snapshots;"
Get-Content .\caput_app.out.log -Tail 80
Get-Content .\caput_app.err.log -Tail 80
```

Keep Windows sleep disabled while the bot is running. If the PC sleeps, the bot and calls stop.
