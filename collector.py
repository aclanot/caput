import json
import os
import time
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
START_URL = os.getenv("START_URL", "https://catapult.trade/turbo/discover")
DEBUG_EVERY_SECONDS = int(os.getenv("DEBUG_EVERY_SECONDS", "120"))
LOCAL_PROFILE_DIR = os.getenv("LOCAL_PROFILE_DIR", "browser_profile")
HEADLESS = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes", "on")

conn = psycopg.connect(DATABASE_URL)
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
CREATE TABLE IF NOT EXISTS graphql_requests (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    url TEXT,
    method TEXT,
    post_data TEXT,
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
        (datetime.utcnow(), event, url, title, body_preview, screenshot)
    )
    print(f"debug: {event} | url={url} | title={title}", flush=True)


def save_graphql(operation, data):
    cur.execute(
        "INSERT INTO graphql_raw(ts, operation, response) VALUES (%s, %s, %s)",
        (datetime.utcnow(), operation, json.dumps(data))
    )


def save_graphql_request(response, data):
    req = response.request
    try:
        post_data = req.post_data
    except Exception:
        post_data = None

    cur.execute(
        """
        INSERT INTO graphql_requests(ts, url, method, post_data, response)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (datetime.utcnow(), response.url, req.method, post_data, json.dumps(data))
    )


def recursive_extract(obj):
    results = []
    if isinstance(obj, dict):
        if 'id' in obj and ('price' in str(obj).lower() or 'mode' in obj or 'speedMode' in obj):
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
        volume = item.get('volume') or item.get('volumeUsdtDrops')
        buys = item.get('buys') or item.get('buysCount')
        sells = item.get('sells') or item.get('sellsCount')
        traders = item.get('traders') or item.get('uniqueTradersCount')
        mode = item.get('mode') or item.get('speedMode')
        cur.execute(
            '''
            INSERT INTO token_snapshots(ts, token_id, mode, price, volume, buys, sells, traders)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ''',
            (datetime.utcnow(), str(token_id), mode, to_float(price), to_float(volume), buys, sells, traders)
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


def attach_handlers(page):
    def on_response(response):
        if '/graphql' not in response.url:
            return
        try:
            data = response.json()
        except Exception as exc:
            print(f"graphql response parse skipped: {exc}", flush=True)
            return

        save_graphql_request(response, data)

        if isinstance(data, list):
            for entry in data:
                save_graphql(None, entry)
                extracted = recursive_extract(entry)
                if extracted:
                    save_snapshots(extracted)
                try_save_finished(entry)
            print("graphql captured batch", flush=True)
            return

        operation = data.get('operationName') if isinstance(data, dict) else None
        save_graphql(operation, data)
        extracted = recursive_extract(data)
        if extracted:
            save_snapshots(extracted)
        try_save_finished(data)
        print(f"graphql captured operation={operation}", flush=True)

    page.on('response', on_response)


print(f"collector boot: START_URL={START_URL}", flush=True)
print(f"profile dir: {Path(LOCAL_PROFILE_DIR).resolve()}", flush=True)
print(f"headless: {HEADLESS}", flush=True)

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=LOCAL_PROFILE_DIR,
        headless=HEADLESS,
        viewport={"width": 1365, "height": 768},
        locale="en-US",
        args=["--disable-blink-features=AutomationControlled"],
    )
    page = context.pages[0] if context.pages else context.new_page()
    attach_handlers(page)
    page.goto(START_URL, wait_until='domcontentloaded', timeout=60000)
    print("Browser opened. If Cloudflare appears, solve it manually in the opened window.", flush=True)
    print("Leave this command window running.", flush=True)
    last_debug = 0
    while True:
        try:
            page.wait_for_timeout(30000)
            now = time.time()
            if now - last_debug >= DEBUG_EVERY_SECONDS:
                log_debug('heartbeat', page=page)
                last_debug = now
        except Exception as exc:
            log_debug('page_loop_error', body_preview=str(exc))
            print(f"page loop error: {exc}", flush=True)
            time.sleep(10)
