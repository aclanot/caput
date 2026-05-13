import json
import os
import time
from datetime import UTC, datetime

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
CREATE TABLE IF NOT EXISTS official_api_token_state (
    token_id TEXT PRIMARY KEY,
    name TEXT,
    symbol TEXT,
    mode TEXT,
    rank TEXT,
    initial_price DOUBLE PRECISION,
    current_price DOUBLE PRECISION,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    volume DOUBLE PRECISION,
    buys INT,
    sells INT,
    traders INT,
    raw JSONB,
    first_seen TIMESTAMP,
    updated_at TIMESTAMP
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


def utcnow():
    return datetime.now(UTC).replace(tzinfo=None)


def parse_api_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


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
        (utcnow(), status, note[:1000]),
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
    return requests.post(API_URL, headers=headers, json=payload, timeout=30)


def save_items(items):
    saved = 0
    now = utcnow()
    for item in items:
        token_id = item.get('id')
        if not token_id:
            continue

        token_id = str(token_id)
        volume = to_float(item.get('volumeUsdtDrops'))
        price = to_float(item.get('price'))
        initial_price = to_float(item.get('initialPrice'))
        buys = to_int(item.get('buysCount'))
        sells = to_int(item.get('sellsCount'))
        traders = to_int(item.get('uniqueTradersCount'))
        start_date = parse_api_datetime(item.get('startDate'))
        end_date = parse_api_datetime(item.get('endDate'))

        cur.execute(
            '''
            INSERT INTO token_snapshots(ts, token_id, mode, price, volume, buys, sells, traders)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ''',
            (
                now,
                token_id,
                item.get('speedMode'),
                price,
                volume,
                buys,
                sells,
                traders,
            ),
        )

        cur.execute(
            '''
            INSERT INTO official_api_token_state(
                token_id, name, symbol, mode, rank,
                initial_price, current_price, start_date, end_date,
                volume, buys, sells, traders, raw, first_seen, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(token_id) DO UPDATE SET
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol,
                mode = EXCLUDED.mode,
                rank = EXCLUDED.rank,
                initial_price = EXCLUDED.initial_price,
                current_price = EXCLUDED.current_price,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                volume = EXCLUDED.volume,
                buys = EXCLUDED.buys,
                sells = EXCLUDED.sells,
                traders = EXCLUDED.traders,
                raw = EXCLUDED.raw,
                updated_at = EXCLUDED.updated_at
            ''',
            (
                token_id,
                item.get('name'),
                item.get('symbol'),
                item.get('speedMode'),
                item.get('rank'),
                initial_price,
                price,
                start_date,
                end_date,
                volume,
                buys,
                sells,
                traders,
                json.dumps(item),
                now,
                now,
            ),
        )

        cur.execute(
            '''
            INSERT INTO official_api_tokens_raw(ts, token_id, response)
            VALUES (%s,%s,%s)
            ''',
            (now, token_id, json.dumps(item)),
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
