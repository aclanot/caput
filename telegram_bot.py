import csv
import io
import os
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone

import psycopg
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')
BACKUP_MAX_ROWS_PER_TABLE = int(os.getenv('BACKUP_MAX_ROWS_PER_TABLE', '0'))
BACKUP_INCLUDE_SNAPSHOTS = os.getenv('BACKUP_INCLUDE_SNAPSHOTS', 'true').lower() in ('1', 'true', 'yes', 'on')
PG_DUMP_TIMEOUT_SECONDS = int(os.getenv('PG_DUMP_TIMEOUT_SECONDS', '600'))
TELEGRAM_MAX_UPLOAD_MB = float(os.getenv('TELEGRAM_MAX_UPLOAD_MB', '45'))
PG_DUMP_ESSENTIAL_TABLES = [
    'finished_tokens',
    'trajectory_features',
    'paper_trades',
    'strategy_sweep',
    'trajectory_clusters',
]
PG_DUMP_CORE_TABLES = [
    'finished_tokens',
    'trajectory_features',
    'paper_trades',
    'strategy_sweep',
    'trajectory_clusters',
    'official_api_token_state',
    'live_token_features',
    'live_signals',
]

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True

BACKUP_TABLES = [
    'finished_tokens',
    'trajectory_features',
    'paper_trades',
    'strategy_sweep',
    'trajectory_clusters',
    'official_api_token_state',
    'token_snapshots',
    'live_token_features',
    'live_signals',
]


def table_exists(cur, table_name):
    cur.execute('SELECT to_regclass(%s)', (f'public.{table_name}',))
    return cur.fetchone()[0] is not None


def existing_tables(table_names):
    cur = conn.cursor()
    return [name for name in table_names if table_exists(cur, name)]


def safe_identifier(name):
    if not name.replace('_', '').isalnum():
        raise ValueError(f'unsafe table name: {name}')
    return name


def utc_stamp():
    return datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')


def write_table_csv_to_zip(cur, zip_file, table_name):
    table_name = safe_identifier(table_name)
    if not table_exists(cur, table_name):
        return 0

    if table_name == 'token_snapshots' and not BACKUP_INCLUDE_SNAPSHOTS:
        return 0

    cur.execute('''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    ''', (table_name,))
    columns = [row[0] for row in cur.fetchall()]
    if not columns:
        return 0

    sql = f'SELECT * FROM {table_name}'
    if BACKUP_MAX_ROWS_PER_TABLE > 0:
        order_col = 'id' if 'id' in columns else columns[0]
        sql += f' ORDER BY {order_col} DESC LIMIT %s'
        cur.execute(sql, (BACKUP_MAX_ROWS_PER_TABLE,))
    else:
        cur.execute(sql)

    rows = cur.fetchall()
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(columns)
    writer.writerows(rows)
    zip_file.writestr(f'{table_name}.csv', csv_buffer.getvalue())
    return len(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Caput bot online\n\n'
        '/status - collector and dataset status\n'
        '/backup - export DB CSV ZIP\n'
        '/pg_dump_essential - smallest PostgreSQL dump, best for Telegram\n'
        '/pg_dump_core - PostgreSQL dump without token_snapshots/logs\n'
        '/pg_dump - full PostgreSQL dump, can be too large for Telegram'
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM finished_tokens')
    finished = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM token_snapshots')
    snapshots = cur.fetchone()[0]
    cur.execute('SELECT MAX(ts) FROM token_snapshots')
    last_snapshot = cur.fetchone()[0]

    features = 0
    if table_exists(cur, 'trajectory_features'):
        cur.execute('SELECT COUNT(*) FROM trajectory_features')
        features = cur.fetchone()[0]

    paper = 0
    if table_exists(cur, 'paper_trades'):
        cur.execute('SELECT COUNT(*) FROM paper_trades')
        paper = cur.fetchone()[0]

    sweep_count = 0
    if table_exists(cur, 'strategy_sweep'):
        cur.execute('SELECT COUNT(*) FROM strategy_sweep')
        sweep_count = cur.fetchone()[0]

    state_count = 0
    if table_exists(cur, 'official_api_token_state'):
        cur.execute('SELECT COUNT(*) FROM official_api_token_state')
        state_count = cur.fetchone()[0]

    text = (
        'Status\n\n'
        f'Finished trajectories: {finished}\n'
        f'Live snapshots: {snapshots}\n'
        f'Official token states: {state_count}\n'
        f'Analysis rows: {features}\n'
        f'Paper simulation trades: {paper}\n'
        f'Sweep rows: {sweep_count}\n'
        f'Last snapshot: {last_snapshot}\n\n'
        'Run order: official_api_collector -> fast_finished_collector -> auto_run.py -> clusters.py'
    )
    await update.message.reply_text(text)


async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    await update.message.reply_text('Creating CSV backup ZIP...')
    zip_buffer = io.BytesIO()
    manifest_lines = [
        'caput backup',
        f'created_utc={datetime.now(timezone.utc).isoformat()}',
        f'backup_max_rows_per_table={BACKUP_MAX_ROWS_PER_TABLE}',
        f'backup_include_snapshots={BACKUP_INCLUDE_SNAPSHOTS}',
        '',
        'tables:',
    ]
    with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        for table_name in BACKUP_TABLES:
            try:
                row_count = write_table_csv_to_zip(cur, zf, table_name)
                if row_count:
                    manifest_lines.append(f'- {table_name}: {row_count} rows')
            except Exception as exc:
                manifest_lines.append(f'- {table_name}: ERROR {exc}')
        zf.writestr('MANIFEST.txt', '\n'.join(manifest_lines) + '\n')
    zip_buffer.seek(0)
    filename = f'caput_backup_{utc_stamp()}.zip'
    await update.message.reply_document(document=zip_buffer, filename=filename)


async def run_pg_dump(update: Update, table_names=None, label='full'):
    await update.message.reply_text(f'Creating PostgreSQL {label} pg_dump backup...')
    selected_tables = existing_tables(table_names) if table_names else None
    if table_names and not selected_tables:
        await update.message.reply_text('No requested tables exist yet, nothing to dump.')
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        dump_path = os.path.join(tmpdir, 'caput_pg_dump.dump')
        dump_cmd = [
            'pg_dump', '--format=custom', '--compress=9', '--no-owner', '--no-privileges',
            DATABASE_URL, '-f', dump_path,
        ]
        if selected_tables:
            for table_name in selected_tables:
                dump_cmd.extend(['--table', f'public.{safe_identifier(table_name)}'])

        try:
            dump_proc = subprocess.run(dump_cmd, capture_output=True, text=True, timeout=PG_DUMP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            await update.message.reply_text(
                f'pg_dump timed out after {PG_DUMP_TIMEOUT_SECONDS}s. Use /pg_dump_essential or increase PG_DUMP_TIMEOUT_SECONDS.'
            )
            return

        if dump_proc.returncode != 0:
            await update.message.reply_text(f'pg_dump failed:\n\n{dump_proc.stderr[-3000:]}')
            return

        filename = f'caput_pg_dump_{label}_{utc_stamp()}.dump'
        size_mb = os.path.getsize(dump_path) / 1024 / 1024
        table_note = ''
        if selected_tables:
            table_note = '\nTables: ' + ', '.join(selected_tables)

        if size_mb > TELEGRAM_MAX_UPLOAD_MB:
            await update.message.reply_text(
                f'pg_dump ready but too large for Telegram: {size_mb:.2f} MB.\n'
                f'Configured Telegram limit: {TELEGRAM_MAX_UPLOAD_MB:.2f} MB.\n'
                f'Use /pg_dump_essential for a smaller backup or store full backups outside Telegram.{table_note}'
            )
            return

        await update.message.reply_text(f'pg_dump ready: {size_mb:.2f} MB. Uploading...{table_note}')
        try:
            with open(dump_path, 'rb') as f:
                await update.message.reply_document(document=f, filename=filename)
        except Exception as exc:
            await update.message.reply_text(f'Telegram upload failed: {exc}')


async def pg_dump_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_pg_dump(update, table_names=None, label='full')


async def pg_dump_core(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_pg_dump(update, table_names=PG_DUMP_CORE_TABLES, label='core')


async def pg_dump_essential(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_pg_dump(update, table_names=PG_DUMP_ESSENTIAL_TABLES, label='essential')


app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler('start', start))
app.add_handler(CommandHandler('status', status))
app.add_handler(CommandHandler('stats', status))
app.add_handler(CommandHandler('backup', backup))
app.add_handler(CommandHandler('pg_dump', pg_dump_backup))
app.add_handler(CommandHandler('pg_dump_core', pg_dump_core))
app.add_handler(CommandHandler('pg_dump_essential', pg_dump_essential))
app.run_polling()
