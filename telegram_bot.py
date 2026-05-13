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
        '/pg_dump - full PostgreSQL dump (.sql.gz)'
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


async def pg_dump_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Creating PostgreSQL pg_dump backup...')

    with tempfile.TemporaryDirectory() as tmpdir:
        sql_path = os.path.join(tmpdir, 'caput_pg_dump.sql')
        gz_path = sql_path + '.gz'

        dump_cmd = [
            'pg_dump',
            '--no-owner',
            '--no-privileges',
            DATABASE_URL,
            '-f',
            sql_path,
        ]

        dump_proc = subprocess.run(
            dump_cmd,
            capture_output=True,
            text=True,
        )

        if dump_proc.returncode != 0:
            err = dump_proc.stderr[-3000:]
            await update.message.reply_text(f'pg_dump failed:\n\n{err}')
            return

        gzip_proc = subprocess.run(
            ['gzip', '-f', sql_path],
            capture_output=True,
            text=True,
        )

        if gzip_proc.returncode != 0:
            err = gzip_proc.stderr[-3000:]
            await update.message.reply_text(f'gzip failed:\n\n{err}')
            return

        filename = f'caput_pg_dump_{utc_stamp()}.sql.gz'

        with open(gz_path, 'rb') as f:
            await update.message.reply_document(document=f, filename=filename)


app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler('start', start))
app.add_handler(CommandHandler('status', status))
app.add_handler(CommandHandler('stats', status))
app.add_handler(CommandHandler('backup', backup))
app.add_handler(CommandHandler('pg_dump', pg_dump_backup))
app.run_polling()
