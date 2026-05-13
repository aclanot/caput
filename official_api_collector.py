import json
import os
import time
from datetime import datetime

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
API_URL = os.getenv('CATAPULT_API_URL', 'https://public-api.catapult.trade/graphql').strip()
API_KEY = os.getenv('CATAPULT_API_KEY', '').strip()
API_INTERVAL_SECONDS = float(os.getenv('API_COLLECTOR_INTERVAL_SECONDS', '10'))
API_LIMIT = float(os.getenv('API_COLLECTOR_LIMIT', '50'))
API_SPEED_MODE = os.getenv('API_COLLECTOR_SPEED_MODE', '').strip().upper()
API_SORT_FIELD = os.getenv('API_COLLECTOR_SORT_FIELD', 'StartTime').strip()
API_SORT_DIRECTION = os.getenv('API_COLLECTOR_SORT_DIRECTION', 'Desc').strip()

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

if not API_KEY:
    raise SystemExit('CATAPULT_API_KEY is missing. Use a read-only key only.')

TOKENS_QUERY = '''
query OfficialApiTokens($input: PublicTokenListInput!) {
  tokens(input: $input) {
    items {
      id
      name
      symbol
      speedMode
      initialPrice
      price
      startDate
      endDate
      buysCount
      sellsCount
      uniqueTradersCount
      volumeUsdtDrops
      rank
    }
    meta {
      firstCursor
      lastCursor
      hasNextItems
      hasPreviousItems
    }
  }
}
'''

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

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
CREATE TABLE IF NOT EXISTS official_api_tokens_raw (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    token_id TEXT,
    response JSONB
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS official_api_collector_log (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP,
    status TEXT,
    note TEXT
)
''')


def to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def to_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def log_status(status, note=''):
    cur.execute(
        'INSERT INTO official_api_collector_log(ts, status, note) VALUES (%s,%s,%s)',
        (datetime.utcnow(), status, note[:1000]),
    )
    print(f'official api collector: {status} | {note[:200]}', flush=True)


def build_input():
    result = {
        'pagination': {'limit': API_LIMIT},
        'sort': {'field': API_SORT_FIELD, 'direction': API_SORT_DIRECTION},
    }
    if API_SPEED_MODE:
        result['filter'] = {'speedMode': [API_SPEED_MODE]}
    return result


def fetch_tokens():
    headers = {
        'accept': 'application/json',
        'content-type': 'application/json',
        'Authorization': f'Bearer {API_KEY}',
    }
    payload = {
        'query': TOKENS_QUERY,
        'operationName': 'OfficialApiTokens',
        'variables': {'input': build_input()},
    }
    response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
    return response


def save_items(items):
    saved = 0
    now = datetime.utcnow()
    for item in items:
        token_id = item.get('id')
        if not token_id:
            continue

        volume = to_float(item.get('volumeUsdtDrops'))
        cur.execute(
            '''
            INSERT INTO token_snapshots(ts, token_id, mode, price, volume, buys, sells, traders)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ''',
            (
                now,
                str(token_id),
                item.get('speedMode'),
                to_float(item.get('price')),
                volume,
                to_int(item.get('buysCount')),
                to_int(item.get('sellsCount')),
                to_int(item.get('uniqueTradersCount')),
            ),
        )
        cur.execute(
            '''
            INSERT INTO official_api_tokens_raw(ts, token_id, response)
            VALUES (%s,%s,%s)
            ''',
            (now, str(token_id), json.dumps(item)),
        )
        saved += 1
    return saved


def main():
    print('official api collector started', flush=True)
    print(f'API_URL={API_URL}', flush=True)
    print(f'interval={API_INTERVAL_SECONDS}s limit={API_LIMIT}', flush=True)
    print(f'sort={API_SORT_FIELD} {API_SORT_DIRECTION} speed_mode={API_SPEED_MODE or "ALL"}', flush=True)
    print('read-only: tokens query only, no mutations', flush=True)

    while True:
        try:
            response = fetch_tokens()
            if response.status_code != 200:
                log_status(f'http_{response.status_code}', response.text[:500])
                time.sleep(API_INTERVAL_SECONDS)
                continue

            data = response.json()
            if data.get('errors'):
                log_status('graphql_errors', json.dumps(data.get('errors'))[:500])
                time.sleep(API_INTERVAL_SECONDS)
                continue

            tokens = data.get('data', {}).get('tokens') or {}
            items = tokens.get('items') or []
            meta = tokens.get('meta') or {}
            saved = save_items(items)
            log_status('saved', f'items={len(items)} snapshots={saved} hasNext={meta.get("hasNextItems")}')

        except Exception as exc:
            log_status('error', str(exc))

        time.sleep(API_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
