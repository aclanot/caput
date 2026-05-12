import json
import os
import time
from datetime import datetime

import psycopg2
from playwright.sync_api import sync_playwright

DATABASE_URL = os.getenv("DATABASE_URL")
START_URL = os.getenv("START_URL", "https://catapult.trade/turbo/discover")
DEBUG_EVERY_SECONDS = int(os.getenv("DEBUG_EVERY_SECONDS", "60"))

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

cur.execute('''
CREATE TABLE IF NOT EXISTS collector_debug (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    event TEXT,
    url TEXT,
    title TEXT,
    body_preview TEXT,
    screenshot BYTEA
)
''')


def log_debug(event, page=None, body_preview=None, screenshot=None):
    url = None
    title = None

    if page is not None:
        try:
            url = page.url
        except Exception:
            pass

        try:
            title = page.title()
        except Exception:
            pass

        if body_preview is None:
            try:
                body_preview = page.locator("body").inner_text(timeout=3000)[:2000]
            except Exception as exc:
                body_preview = f"body read failed: {exc}"

        if screenshot is None:
            try:
                screenshot = page.screenshot(full_page=False)
            except Exception:
                screenshot = None

    cur.execute(
        """
        INSERT INTO collector_debug(ts, event, url, title, body_preview, screenshot)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (datetime.utcnow(), event, url, title, body_preview, psycopg2.Binary(screenshot) if screenshot else None)
    )

    print(f"debug: {event} | url={url} | title={title}", flush=True)


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


def to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def save_snapshots(items):
    saved = 0

    for item in items:
        token_id = item.get('id') or item.get('tokenId')

        if not token_id:
            continue

        price = item.get('currentPrice') or item.get('price') or item.get('markPrice')
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
                datetime.utcnow(), str(token_id), mode,
                to_float(price), to_float(volume), buys, sells, traders,
            )
        )
        saved += 1

    if saved:
        print(f"snapshots saved: {saved}", flush=True)


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
                    INSERT INTO finished_tokens(token_id, ts, salt, ticks)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (token_id) DO NOTHING
                    ''',
                    (str(token_id), datetime.utcnow(), salt, json.dumps(ticks))
                )
                print(f"finished token saved: {token_id}", flush=True)

            for v in x.values():
                walk(v)

        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(data)


print(f"collector boot: START_URL={START_URL}", flush=True)

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )

    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1365, "height": 768},
        locale="en-US",
    )

    page = context.new_page()

    def on_response(response):
        if '/graphql' not in response.url:
            return

        try:
            data = response.json()
        except Exception as exc:
            print(f"graphql response parse failed: {exc}", flush=True)
            return

        operation = None
        if isinstance(data, dict):
            operation = data.get('operationName')

        save_graphql(operation, data)
        extracted = recursive_extract(data)

        if extracted:
            save_snapshots(extracted)

        try_save_finished(data)
        print(f"graphql captured operation={operation}", flush=True)

    page.on('response', on_response)

    try:
        page.goto(START_URL, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(10000)
        log_debug('initial_load', page=page)
    except Exception as exc:
        log_debug('initial_load_failed', body_preview=str(exc))
        print(f"initial load failed: {exc}", flush=True)

    last_debug = 0

    while True:
        try:
            page.reload(wait_until='domcontentloaded', timeout=60000)
            page.wait_for_timeout(8000)

            now = time.time()
            if now - last_debug >= DEBUG_EVERY_SECONDS:
                log_debug('periodic_reload', page=page)
                last_debug = now

        except Exception as exc:
            log_debug('reload_failed', body_preview=str(exc))
            print(f"reload failed: {exc}", flush=True)

        time.sleep(15)
