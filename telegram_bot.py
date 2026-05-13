import csv
import io
import os
import zipfile
from datetime import UTC, datetime

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
    return datetime.now(UTC).strftime('%Y%m%d_%H%M%S')


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
        '/summary - pattern summary by mode\n'
        '/paper - paper simulation summary\n'
        '/sweep - top optimized parameter sets\n'
        '/latest - latest saved trajectories\n'
        '/backup - export DB backup ZIP\n'
        '/export - export finished trajectories CSV\n'
        '/export_features - export analysis features CSV\n'
        '/export_sweep - export sweep results CSV'
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
    elif table_exists(cur, 'virtual_trades'):
        cur.execute('SELECT COUNT(*) FROM virtual_trades')
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


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()

    if not table_exists(cur, 'trajectory_features'):
        await update.message.reply_text('No analysis table yet. Run: python analyze.py')
        return

    cur.execute('''
    SELECT
        COALESCE(mode, 'UNKNOWN') AS mode,
        COUNT(*) AS n,
        AVG(final_return_pct) AS avg_final,
        AVG(max_pump_pct) AS avg_pump,
        AVG(max_drawdown_pct) AS avg_dd,
        AVG(CASE WHEN final_return_pct < 0 THEN 1.0 ELSE 0.0 END) AS p_down,
        AVG(CASE WHEN roundtrip_after_90dd THEN 1.0 ELSE 0.0 END) AS p_roundtrip,
        AVG(CASE WHEN dead_bounce_20 THEN 1.0 ELSE 0.0 END) AS p_bounce,
        AVG(CASE WHEN pump30_dump20 THEN 1.0 ELSE 0.0 END) AS p_pump_fail
    FROM trajectory_features
    GROUP BY COALESCE(mode, 'UNKNOWN')
    ORDER BY n DESC
    ''')
    rows = cur.fetchall()

    if not rows:
        await update.message.reply_text('No analysis rows yet. Run: python analyze.py')
        return

    text = 'Pattern summary by mode\n\n'
    for r in rows:
        text += (
            f'{r[0]} | n={r[1]}\n'
            f'avg final: {r[2]:.1f}% | avg pump: {r[3]:.1f}% | avg drawdown: {r[4]:.1f}%\n'
            f'final below start: {r[5]*100:.1f}%\n'
            f'roundtrip after -90%: {r[6]*100:.1f}%\n'
            f'bounce after -90%: {r[7]*100:.1f}%\n'
            f'pump +30 then fail: {r[8]*100:.1f}%\n\n'
        )

    await update.message.reply_text(text[:4000])


async def paper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()

    table = None
    if table_exists(cur, 'paper_trades'):
        table = 'paper_trades'
    elif table_exists(cur, 'virtual_trades'):
        table = 'virtual_trades'

    if not table:
        await update.message.reply_text('No paper simulation table yet. Run: python paper_sim.py')
        return

    cur.execute(f'''
    SELECT
        strategy,
        COALESCE(mode, 'UNKNOWN') AS mode,
        COUNT(*) AS n,
        AVG(pnl_pct) AS avg_pnl,
        AVG(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0.0 END) AS winrate,
        MIN(pnl_pct) AS worst,
        MAX(pnl_pct) AS best
    FROM {table}
    GROUP BY strategy, COALESCE(mode, 'UNKNOWN')
    ORDER BY avg_pnl DESC
    LIMIT 20
    ''')
    rows = cur.fetchall()

    if not rows:
        await update.message.reply_text('No paper simulation rows yet.')
        return

    text = 'Paper simulation summary\n\n'
    for r in rows:
        text += (
            f'{r[0]} / {r[1]} | n={r[2]}\n'
            f'avg pnl: {r[3]:.2f}% | winrate: {r[4]*100:.1f}%\n'
            f'worst: {r[5]:.2f}% | best: {r[6]:.2f}%\n\n'
        )

    await update.message.reply_text(text[:4000])


async def sweep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()

    if not table_exists(cur, 'strategy_sweep'):
        await update.message.reply_text('No sweep results yet. Run: python sweep.py')
        return

    min_trades = 10
    if context.args:
        try:
            min_trades = int(context.args[0])
        except Exception:
            min_trades = 10

    cur.execute('''
    SELECT mode, side, strategy, trades, winrate, avg_pnl, median_pnl, worst_pnl, best_pnl
    FROM strategy_sweep
    WHERE trades >= %s
    ORDER BY avg_pnl DESC
    LIMIT 15
    ''', (min_trades,))
    rows = cur.fetchall()

    if not rows:
        await update.message.reply_text(f'No sweep rows with trades >= {min_trades}.')
        return

    text = f'Top sweep results, min trades={min_trades}\n\n'
    for r in rows:
        text += (
            f'{r[0]} {r[1]}\n'
            f'{r[2]} | n={r[3]}\n'
            f'avg: {r[5]:.2f}% | median: {r[6]:.2f}% | win: {r[4]*100:.1f}%\n'
            f'worst: {r[7]:.2f}% | best: {r[8]:.2f}%\n\n'
        )

    await update.message.reply_text(text[:4000])


async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('''
    SELECT ft.token_id, ft.ts, tf.mode, tf.final_return_pct, tf.max_pump_pct, tf.max_drawdown_pct
    FROM finished_tokens ft
    LEFT JOIN trajectory_features tf ON tf.token_id = ft.token_id
    ORDER BY ft.ts DESC
    LIMIT 10
    ''')
    rows = cur.fetchall()

    if not rows:
        await update.message.reply_text('No finished trajectories yet.')
        return

    text = 'Latest trajectories\n\n'
    for r in rows:
        if r[3] is None:
            text += f'{r[0]} | {r[1]} | not analyzed yet\n'
        else:
            text += f'{r[0]} | {r[2]} | final {r[3]:.1f}% | pump {r[4]:.1f}% | dd {r[5]:.1f}%\n'

    await update.message.reply_text(text[:4000])


async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    await update.message.reply_text('Creating backup ZIP...')

    zip_buffer = io.BytesIO()
    manifest_lines = [
        'caput backup',
        f'created_utc={datetime.now(UTC).isoformat()}',
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


async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('SELECT token_id, ts, salt, ticks::text FROM finished_tokens ORDER BY ts DESC LIMIT 2000')
    rows = cur.fetchall()

    csv_bytes = io.StringIO()
    writer = csv.writer(csv_bytes)
    writer.writerow(['token_id', 'ts', 'salt', 'ticks'])
    writer.writerows(rows)

    data = io.BytesIO(csv_bytes.getvalue().encode('utf-8'))
    data.seek(0)
    await update.message.reply_document(document=data, filename='finished_tokens.csv')


async def export_features(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()

    if not table_exists(cur, 'trajectory_features'):
        await update.message.reply_text('No analysis table yet. Run: python analyze.py')
        return

    cur.execute('''
    SELECT token_id, mode, ticks_count, final_return_pct, max_pump_pct, max_drawdown_pct,
           roundtrip_after_90dd, dead_bounce_20, pump30_dump20
    FROM trajectory_features
    ORDER BY updated_at DESC
    LIMIT 5000
    ''')
    rows = cur.fetchall()

    csv_bytes = io.StringIO()
    writer = csv.writer(csv_bytes)
    writer.writerow([
        'token_id', 'mode', 'ticks_count', 'final_return_pct', 'max_pump_pct', 'max_drawdown_pct',
        'roundtrip_after_90dd', 'dead_bounce_20', 'pump30_dump20'
    ])
    writer.writerows(rows)

    data = io.BytesIO(csv_bytes.getvalue().encode('utf-8'))
    data.seek(0)
    await update.message.reply_document(document=data, filename='trajectory_features.csv')


async def export_sweep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()

    if not table_exists(cur, 'strategy_sweep'):
        await update.message.reply_text('No sweep results yet. Run: python sweep.py')
        return

    cur.execute('''
    SELECT mode, side, strategy, entry_threshold, take_profit, stop_loss, trades,
           winrate, avg_pnl, median_pnl, worst_pnl, best_pnl, expectancy
    FROM strategy_sweep
    ORDER BY avg_pnl DESC
    LIMIT 5000
    ''')
    rows = cur.fetchall()

    csv_bytes = io.StringIO()
    writer = csv.writer(csv_bytes)
    writer.writerow([
        'mode', 'side', 'strategy', 'entry_threshold', 'take_profit', 'stop_loss', 'trades',
        'winrate', 'avg_pnl', 'median_pnl', 'worst_pnl', 'best_pnl', 'expectancy'
    ])
    writer.writerows(rows)

    data = io.BytesIO(csv_bytes.getvalue().encode('utf-8'))
    data.seek(0)
    await update.message.reply_document(document=data, filename='strategy_sweep.csv')


app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler('start', start))
app.add_handler(CommandHandler('status', status))
app.add_handler(CommandHandler('stats', status))
app.add_handler(CommandHandler('summary', summary))
app.add_handler(CommandHandler('paper', paper))
app.add_handler(CommandHandler('sweep', sweep))
app.add_handler(CommandHandler('latest', latest))
app.add_handler(CommandHandler('backup', backup))
app.add_handler(CommandHandler('export', export))
app.add_handler(CommandHandler('export_features', export_features))
app.add_handler(CommandHandler('export_sweep', export_sweep))
app.run_polling()
