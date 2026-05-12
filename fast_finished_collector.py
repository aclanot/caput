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
LOCAL_PROFILE_DIR = os.getenv('LOCAL_PROFILE_DIR', 'browser_profile_fast')
HEADLESS = os.getenv('HEADLESS', 'false').lower() in ('1', 'true', 'yes', 'on')
START_URL = os.getenv('START_URL', 'https://catapult.trade/turbo/discover')
REQUEST_INTERVAL_SECONDS = float(os.getenv('FAST_FINISHED_INTERVAL_SECONDS', '0.15'))
BATCH_LIMIT = int(os.getenv('FAST_FINISHED_BATCH_LIMIT', '200'))

FAIR_QUERY = '''
query TurboTokenFairData($tokenId: String!) {
  turboTokenFairData(tokenId: $tokenId) {
    fairHash
    fairSalt
    speedTicksInSecond
    ticksArray
    __typename
  }
}
'''

DETAILS_QUERY = '''
query TurboTokenDetailsV2($tokenId: String!) {
  turboTokenDetailsV2(tokenId: $tokenId) {
    id
    name
    symbol
    endDate
    initialPrice
    isExpired
    speedMode
    startDate
    __typename
  }
}
'''

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
CREATE TABLE IF NOT EXISTS fast_finished_scan_log (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    token_id TEXT,
    status TEXT,
    note TEXT
)
''')


def log_scan(token_id, status, note=''):
    cur.execute(
        'INSERT INTO fast_finished_scan_log(ts, token_id, status, note) VALUES (%s,%s,%s,%s)',
        (datetime.utcnow(), str(token_id), status, note[:1000])
    )
    print(f'fast scan: {token_id} | {status} | {note[:120]}', flush=True)


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
    ''', (BATCH_LIMIT,))
    return [row[0] for row in cur.fetchall()]


def build_payload(token_id):
    return [
        {
            'query': FAIR_QUERY,
            'variables': {'tokenId': str(token_id)},
            'operationName': 'TurboTokenFairData',
        },
        {
            'query': DETAILS_QUERY,
            'variables': {'tokenId': str(token_id)},
            'operationName': 'TurboTokenDetailsV2',
        },
    ]


def save_finished(token_id, fair):
    salt = fair.get('fairSalt')
    ticks = fair.get('ticksArray')
    if not salt or not ticks:
        return False

    cur.execute('''
    INSERT INTO finished_tokens(token_id, ts, salt, ticks)
    VALUES (%s,%s,%s,%s)
    ON CONFLICT (token_id) DO NOTHING
    ''', (str(token_id), datetime.utcnow(), salt, json.dumps(ticks)))
    return True


def fetch_fair_data(page, token_id):
    payload = build_payload(token_id)
    return page.evaluate(
        '''async ({payload}) => {
            const res = await fetch('/graphql', {
                method: 'POST',
                headers: {
                    'accept': '*/*',
                    'content-type': 'application/json'
                },
                body: JSON.stringify(payload),
                credentials: 'include'
            });
            const text = await res.text();
            return {status: res.status, text};
        }''',
        {'payload': payload},
    )


print('fast finished browser collector started', flush=True)
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

    page = context.pages[0] if context.pages else context.new_page()
    page.goto(START_URL, wait_until='domcontentloaded', timeout=60000)
    page.wait_for_timeout(8000)

    print('Browser ready. If Cloudflare appears, solve it manually in the opened window.', flush=True)

    while True:
        tokens = get_candidate_tokens()

        if not tokens:
            print('no token candidates yet', flush=True)
            time.sleep(5)
            continue

        for token_id in tokens:
            try:
                result = fetch_fair_data(page, token_id)
                status = result.get('status')
                text = result.get('text') or ''

                if status != 200:
                    log_scan(token_id, f'http_{status}', text[:300])
                    time.sleep(2)
                    continue

                data = json.loads(text)
                fair = None
                details = None

                if isinstance(data, list):
                    for entry in data:
                        if entry.get('data', {}).get('turboTokenFairData'):
                            fair = entry['data']['turboTokenFairData']
                        if entry.get('data', {}).get('turboTokenDetailsV2'):
                            details = entry['data']['turboTokenDetailsV2']

                if not fair:
                    log_scan(token_id, 'no_fair_data', 'missing turboTokenFairData')
                    continue

                if save_finished(token_id, fair):
                    ticks_len = len(fair.get('ticksArray') or [])
                    expired = details.get('isExpired') if details else None
                    mode = details.get('speedMode') if details else None
                    log_scan(token_id, 'saved', f'ticks={ticks_len}; expired={expired}; mode={mode}')
                else:
                    log_scan(token_id, 'not_ready', 'no fairSalt or ticksArray')

            except Exception as exc:
                log_scan(token_id, 'error', str(exc))

            time.sleep(REQUEST_INTERVAL_SECONDS)

        time.sleep(2)
