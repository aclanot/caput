import json
import os
import time
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
LOCAL_PROFILE_DIR = os.getenv('LOCAL_PROFILE_DIR', 'browser_profile')
HEADLESS = os.getenv('HEADLESS', 'false').lower() in ('1', 'true', 'yes', 'on')
CHECK_INTERVAL_SECONDS = int(os.getenv('FINISHED_CHECK_INTERVAL_SECONDS', '20'))
TOKEN_LIMIT = int(os.getenv('FINISHED_TOKEN_LIMIT', '25'))

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS finished_tokens (
    token_id TEXT PRIMARY KEY,
    ts TIMESTAMP,
    salt TEXT,
    ticks JSONB
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS finished_scan_log (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    token_id TEXT,
    status TEXT,
    note TEXT
)
''')


def log_scan(token_id, status, note=''):
    cur.execute(
        'INSERT INTO finished_scan_log(ts, token_id, status, note) VALUES (%s,%s,%s,%s)',
        (datetime.utcnow(), str(token_id), status, note[:1000])
    )
    print(f'finished scan: {token_id} | {status} | {note[:120]}', flush=True)


def get_candidate_tokens():
    cur.execute('''
    SELECT token_id
    FROM (
        SELECT token_id, MAX(token_id::BIGINT) AS token_num
        FROM token_snapshots
        WHERE token_id IS NOT NULL
          AND token_id ~ '^[0-9]+$'
          AND token_id NOT IN (SELECT token_id FROM finished_tokens)
        GROUP BY token_id
    ) t
    ORDER BY token_num DESC
    LIMIT %s
    ''', (TOKEN_LIMIT,))
    return [row[0] for row in cur.fetchall()]


def save_finished(token_id, salt, ticks):
    cur.execute('''
    INSERT INTO finished_tokens(token_id, ts, salt, ticks)
    VALUES (%s,%s,%s,%s)
    ON CONFLICT (token_id) DO NOTHING
    ''', (str(token_id), datetime.utcnow(), salt, json.dumps(ticks)))


def find_finished_payload(obj, fallback_token_id=None):
    hits = []

    def walk(x):
        if isinstance(x, dict):
            salt = x.get('salt')
            ticks = x.get('ticks')
            token_id = x.get('id') or x.get('tokenId') or fallback_token_id

            if token_id and salt and ticks:
                hits.append((str(token_id), salt, ticks))

            for v in x.values():
                walk(v)

        elif isinstance(x, list):
            for item in x:
                walk(item)

    walk(obj)
    return hits


def attach_handlers(page, expected_token_id):
    def on_response(response):
        if '/graphql' not in response.url:
            return

        try:
            data = response.json()
        except Exception:
            return

        payloads = []
        if isinstance(data, list):
            for item in data:
                payloads.extend(find_finished_payload(item, expected_token_id))
        else:
            payloads.extend(find_finished_payload(data, expected_token_id))

        for token_id, salt, ticks in payloads:
            save_finished(token_id, salt, ticks)
            log_scan(token_id, 'saved', f'ticks={len(ticks) if hasattr(ticks, "__len__") else "unknown"}')

    page.on('response', on_response)


print('finished collector started', flush=True)
print(f'profile dir: {Path(LOCAL_PROFILE_DIR).resolve()}', flush=True)
print(f'headless: {HEADLESS}', flush=True)

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=LOCAL_PROFILE_DIR,
        headless=HEADLESS,
        viewport={'width': 1365, 'height': 768},
        locale='en-US',
        args=['--disable-blink-features=AutomationControlled'],
    )

    while True:
        tokens = get_candidate_tokens()

        if not tokens:
            print('no token candidates yet', flush=True)
            time.sleep(CHECK_INTERVAL_SECONDS)
            continue

        for token_id in tokens:
            page = context.new_page()
            attach_handlers(page, token_id)

            url = f'https://catapult.trade/turbo/tokens/{token_id}'
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=60000)
                page.wait_for_timeout(12000)
                log_scan(token_id, 'visited', page.title())
            except Exception as exc:
                log_scan(token_id, 'error', str(exc))
            finally:
                try:
                    page.close()
                except Exception:
                    pass

            time.sleep(2)

        time.sleep(CHECK_INTERVAL_SECONDS)
