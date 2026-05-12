import io
import os

import pandas as pd
import psycopg2
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')

conn = psycopg2.connect(DATABASE_URL)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Caput bot online\n\n'
        '/stats - dataset stats\n'
        '/latest - latest finished tokens\n'
        '/export - export finished trajectories csv'
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()

    cur.execute('SELECT COUNT(*) FROM finished_tokens')
    finished = cur.fetchone()[0]

    cur.execute('SELECT COUNT(*) FROM token_snapshots')
    snaps = cur.fetchone()[0]

    await update.message.reply_text(
        f'Finished trajectories: {finished}\n'
        f'Snapshots: {snaps}'
    )


async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()

    cur.execute('''
    SELECT token_id, ts
    FROM finished_tokens
    ORDER BY ts DESC
    LIMIT 10
    ''')

    rows = cur.fetchall()

    if not rows:
        await update.message.reply_text('No finished tokens yet')
        return

    text = 'Latest finished tokens:\n\n'

    for r in rows:
        text += f'{r[0]} | {r[1]}\n'

    await update.message.reply_text(text)


async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = '''
    SELECT * FROM finished_tokens
    ORDER BY ts DESC
    LIMIT 1000
    '''

    df = pd.read_sql(query, conn)

    csv_bytes = io.BytesIO()

    df.to_csv(csv_bytes, index=False)

    csv_bytes.seek(0)

    await update.message.reply_document(
        document=csv_bytes,
        filename='finished_tokens.csv'
    )


app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler('start', start))
app.add_handler(CommandHandler('stats', stats))
app.add_handler(CommandHandler('latest', latest))
app.add_handler(CommandHandler('export', export))

app.run_polling()
