import json
import os
import time
from datetime import datetime

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
CF_CLEARANCE = os.getenv('CF_CLEARANCE', '').strip()
CATAPULT_COOKIE = os.getenv('CATAPULT_COOKIE', '').strip()
REQUEST_INTERVAL_SECONDS = float(os.getenv('FAST_FINISHED_INTERVAL_SECONDS', '0.25'))
BATCH_LIMIT = int(os.getenv('FAST_FINISHED_BATCH_LIMIT', '200'))

GRAPHQL_URL = 'https://catapult.trade/graphql'

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

HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://catapult.trade',
    'referer': 'https://catapult.trade/turbo/discover',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/125.0.0.0 Safari/537.36'
    ),
}

if CATAPULT_COOKIE:
    HEADERS['cookie'] = CATAPULT_COOKIE
elif CF_CLEARANCE:
    HEADERS['cookie'] = f'cf_clearance={CF_CLEARANCE}'

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

session = requests.Session()


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


print('fast finished collector started', flush=True)
print(f'cookie configured: {bool(HEADERS.get("cookie"))}', flush=True)

while True:
    tokens = get_candidate_tokens()

    if not tokens:
        print('no token candidates yet', flush=True)
        time.sleep(5)
        continue

    for token_id in tokens:
        try:
            r = session.post(
                GRAPHQL_URL,
                headers=HEADERS,
                json=build_payload(token_id),
                timeout=30,
            )

            if r.status_code != 200:
                log_scan(token_id, f'http_{r.status_code}', r.text[:300])
                time.sleep(2)
                continue

            data = r.json()

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
