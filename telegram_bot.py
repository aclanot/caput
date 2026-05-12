import csv
import io
import json
import os

import psycopg
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True


def short_json(value, limit=3500):
    try:
        text = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    except Exception:
        text = str(value)
    if len(text) > limit:
        return text[:limit] + '\n... truncated ...'
    return text


def short_text(value, limit=3500):
    text = value or ''
    if len(text) > limit:
        return text[:limit] + '\n... truncated ...'
    return text


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Caput bot online\n\n'
        '/stats - dataset stats\n'
        '/raw - raw GraphQL count\n'
        '/ops - recent GraphQL operations\n'
        '/lastraw - latest raw GraphQL response preview\n'
        '/reqs - raw GraphQL request count\n'
        '/lastreq - latest GraphQL request payload\n'
        '/findticks - latest GraphQL requests/responses containing ticks or salt\n'
        '/debug - latest collector debug\n'
        '/latest - latest finished tokens\n'
        '/export - export finished trajectories csv\n'
        '/export_snapshots - export latest snapshots csv'
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM finished_tokens')
    finished = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM token_snapshots')
    snaps = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM graphql_raw')
    raw = cur.fetchone()[0]
    cur.execute("SELECT to_regclass('public.graphql_requests')")
    has_reqs = cur.fetchone()[0]
    reqs = 0
    if has_reqs:
        cur.execute('SELECT COUNT(*) FROM graphql_requests')
        reqs = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM collector_debug')
    debug = cur.fetchone()[0]
    cur.execute('SELECT MAX(ts) FROM token_snapshots')
    last_snapshot = cur.fetchone()[0]
    await update.message.reply_text(
        f'Finished trajectories: {finished}\n'
        f'Snapshots: {snaps}\n'
        f'Raw GraphQL: {raw}\n'
        f'GraphQL requests: {reqs}\n'
        f'Debug rows: {debug}\n'
        f'Last snapshot: {last_snapshot}'
    )


async def raw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM graphql_raw')
    count = cur.fetchone()[0]
    await update.message.reply_text(f'Raw GraphQL rows: {count}')


async def reqs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('public.graphql_requests')")
    if not cur.fetchone()[0]:
        await update.message.reply_text('graphql_requests table does not exist yet')
        return
    cur.execute('SELECT COUNT(*) FROM graphql_requests')
    count = cur.fetchone()[0]
    await update.message.reply_text(f'GraphQL request rows: {count}')


async def ops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('''
    SELECT COALESCE(operation, 'NULL') AS op, COUNT(*)
    FROM graphql_raw
    GROUP BY COALESCE(operation, 'NULL')
    ORDER BY COUNT(*) DESC
    LIMIT 20
    ''')
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No GraphQL rows yet')
        return
    text = 'GraphQL operations:\n\n'
    for op, count in rows:
        text += f'{op}: {count}\n'
    await update.message.reply_text(text)


async def lastraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('''
    SELECT id, ts, operation, response
    FROM graphql_raw
    ORDER BY id DESC
    LIMIT 1
    ''')
    row = cur.fetchone()
    if not row:
        await update.message.reply_text('No raw GraphQL rows yet')
        return
    msg = f'id: {row[0]}\nts: {row[1]}\noperation: {row[2]}\n\n{short_json(row[3])}'
    await update.message.reply_text(msg[:4000])


async def lastreq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('public.graphql_requests')")
    if not cur.fetchone()[0]:
        await update.message.reply_text('graphql_requests table does not exist yet')
        return
    cur.execute('''
    SELECT id, ts, url, method, post_data, response
    FROM graphql_requests
    ORDER BY id DESC
    LIMIT 1
    ''')
    row = cur.fetchone()
    if not row:
        await update.message.reply_text('No GraphQL request rows yet')
        return
    msg = (
        f'id: {row[0]}\n'
        f'ts: {row[1]}\n'
        f'method: {row[3]}\n'
        f'url: {row[2]}\n\n'
        f'POST DATA:\n{short_text(row[4], 1800)}\n\n'
        f'RESPONSE:\n{short_json(row[5], 1800)}'
    )
    await update.message.reply_text(msg[:4000])


async def findticks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('public.graphql_requests')")
    if not cur.fetchone()[0]:
        await update.message.reply_text('graphql_requests table does not exist yet')
        return
    cur.execute('''
    SELECT id, ts, post_data, response
    FROM graphql_requests
    WHERE COALESCE(post_data, '') ILIKE '%tick%'
       OR COALESCE(post_data, '') ILIKE '%salt%'
       OR response::text ILIKE '%tick%'
       OR response::text ILIKE '%salt%'
    ORDER BY id DESC
    LIMIT 5
    ''')
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No GraphQL requests/responses with tick/salt yet')
        return
    text = 'tick/salt candidates:\n\n'
    for row in rows:
        pd = (row[2] or '')[:500].replace('\n', ' ')
        resp = short_json(row[3], 700).replace('\n', ' ')
        text += f'#{row[0]} {row[1]}\nPOST: {pd}\nRESP: {resp}\n\n'
    await update.message.reply_text(text[:4000])


async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('''
    SELECT id, ts, event, url, title, body_preview
    FROM collector_debug
    ORDER BY id DESC
    LIMIT 5
    ''')
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No debug rows yet')
        return
    text = 'Latest debug rows:\n\n'
    for row in rows:
        body = (row[5] or '')[:500].replace('\n', ' ')
        text += f'#{row[0]} {row[1]}\n{row[2]} | {row[4]}\n{row[3]}\n{body}\n\n'
    await update.message.reply_text(text[:4000])


async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('SELECT token_id, ts FROM finished_tokens ORDER BY ts DESC LIMIT 10')
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No finished tokens yet')
        return
    text = 'Latest finished tokens:\n\n'
    for r in rows:
        text += f'{r[0]} | {r[1]}\n'
    await update.message.reply_text(text)


async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('SELECT token_id, ts, salt, ticks::text FROM finished_tokens ORDER BY ts DESC LIMIT 1000')
    rows = cur.fetchall()
    csv_bytes = io.StringIO()
    writer = csv.writer(csv_bytes)
    writer.writerow(['token_id', 'ts', 'salt', 'ticks'])
    writer.writerows(rows)
    data = io.BytesIO(csv_bytes.getvalue().encode('utf-8'))
    data.seek(0)
    await update.message.reply_document(document=data, filename='finished_tokens.csv')


async def export_snapshots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('''
    SELECT ts, token_id, mode, price, volume, buys, sells, traders
    FROM token_snapshots
    ORDER BY id DESC
    LIMIT 5000
    ''')
    rows = cur.fetchall()
    csv_bytes = io.StringIO()
    writer = csv.writer(csv_bytes)
    writer.writerow(['ts', 'token_id', 'mode', 'price', 'volume', 'buys', 'sells', 'traders'])
    writer.writerows(rows)
    data = io.BytesIO(csv_bytes.getvalue().encode('utf-8'))
    data.seek(0)
    await update.message.reply_document(document=data, filename='token_snapshots.csv')


app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler('start', start))
app.add_handler(CommandHandler('stats', stats))
app.add_handler(CommandHandler('raw', raw))
app.add_handler(CommandHandler('reqs', reqs))
app.add_handler(CommandHandler('ops', ops))
app.add_handler(CommandHandler('lastraw', lastraw))
app.add_handler(CommandHandler('lastreq', lastreq))
app.add_handler(CommandHandler('findticks', findticks))
app.add_handler(CommandHandler('debug', debug))
app.add_handler(CommandHandler('latest', latest))
app.add_handler(CommandHandler('export', export))
app.add_handler(CommandHandler('export_snapshots', export_snapshots))
app.run_polling()
