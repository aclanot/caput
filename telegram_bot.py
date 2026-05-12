import csv
import io
import os

import psycopg
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True


def table_exists(cur, table_name):
    cur.execute('SELECT to_regclass(%s)', (f'public.{table_name}',))
    return cur.fetchone()[0] is not None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Caput bot online\n\n'
        '/status - collector and dataset status\n'
        '/summary - pattern summary by mode\n'
        '/paper - paper simulation summary\n'
        '/latest - latest saved trajectories\n'
        '/export - export finished trajectories CSV\n'
        '/export_features - export analysis features CSV'
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

    text = (
        'Status\n\n'
        f'Finished trajectories: {finished}\n'
        f'Live snapshots: {snapshots}\n'
        f'Analysis rows: {features}\n'
        f'Paper simulation trades: {paper}\n'
        f'Last snapshot: {last_snapshot}\n\n'
        'If finished trajectories are not growing, the collector is probably waiting for new expired tokens.'
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
        await update.message.reply_text('No paper simulation table yet. Run the paper/backtest script after analyze.py.')
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
    ORDER BY strategy, n DESC
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


app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler('start', start))
app.add_handler(CommandHandler('status', status))
app.add_handler(CommandHandler('stats', status))
app.add_handler(CommandHandler('summary', summary))
app.add_handler(CommandHandler('paper', paper))
app.add_handler(CommandHandler('latest', latest))
app.add_handler(CommandHandler('export', export))
app.add_handler(CommandHandler('export_features', export_features))
app.run_polling()
