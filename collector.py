import json
import os
import time
from datetime import datetime

import psycopg2
from playwright.sync_api import sync_playwright

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True

cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS graphql_raw (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    operation TEXT,
    response JSONB
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS token_snapshots (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    token_id TEXT,
    mode TEXT,
    price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    buys INT,
    sells INT,
    traders INT
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS finished_tokens (
    token_id TEXT PRIMARY KEY,
    ts TIMESTAMP,
    salt TEXT,
    ticks JSONB
)
''')


def save_graphql(operation, data):
    cur.execute(
        "INSERT INTO graphql_raw(ts, operation, response) VALUES (%s, %s, %s)",
        (datetime.utcnow(), operation, json.dumps(data))
    )


def recursive_extract(obj):
    results = []

    if isinstance(obj, dict):
        if 'id' in obj and ('price' in str(obj).lower() or 'mode' in obj):
            results.append(obj)

        for v in obj.values():
            results.extend(recursive_extract(v))

    elif isinstance(obj, list):
        for item in obj:
            results.extend(recursive_extract(item))

    return results


def save_snapshots(items):
    for item in items:
        token_id = item.get('id') or item.get('tokenId')

        if not token_id:
            continue

        price = (
            item.get('currentPrice')
            or item.get('price')
            or item.get('markPrice')
        )

        volume = item.get('volume')
        buys = item.get('buys')
        sells = item.get('sells')
        traders = item.get('traders')
        mode = item.get('mode')

        cur.execute(
            '''
            INSERT INTO token_snapshots(
                ts, token_id, mode, price,
                volume, buys, sells, traders
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ''',
            (
                datetime.utcnow(),
                str(token_id),
                mode,
                float(price) if price else None,
                float(volume) if volume else None,
                buys,
                sells,
                traders,
            )
        )


def try_save_finished(data):
    if not isinstance(data, dict):
        return

    text = json.dumps(data).lower()

    if 'ticks' not in text:
        return

    def walk(x):
        if isinstance(x, dict):
            token_id = x.get('id') or x.get('tokenId')
            salt = x.get('salt')
            ticks = x.get('ticks')

            if token_id and salt and ticks:
                cur.execute(
                    '''
                    INSERT INTO finished_tokens(
                        token_id, ts, salt, ticks
                    ) VALUES (%s,%s,%s,%s)
                    ON CONFLICT (token_id) DO NOTHING
                    ''',
                    (
                        str(token_id),
                        datetime.utcnow(),
                        salt,
                        json.dumps(ticks)
                    )
                )

            for v in x.values():
                walk(v)

        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(data)


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)

    context = browser.new_context()
    page = context.new_page()

    def on_response(response):
        if '/graphql' not in response.url:
            return

        try:
            data = response.json()
        except Exception:
            return

        operation = None

        if isinstance(data, dict):
            operation = data.get('operationName')

        save_graphql(operation, data)

        extracted = recursive_extract(data)

        if extracted:
            save_snapshots(extracted)

        try_save_finished(data)

        print('graphql captured')

    page.on('response', on_response)

    page.goto('https://catapult.trade/turbo/discover')

    while True:
        page.reload(wait_until='domcontentloaded')
        time.sleep(15)
