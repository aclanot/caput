import json
import os
import time
from datetime import datetime

import psycopg2
import requests

DATABASE_URL = os.getenv('DATABASE_URL')

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

GRAPHQL_URL = 'https://catapult.trade/graphql'

QUERY = '''
query TurboTokenList($pagination: CursorPaginationInput!, $filter: TurboTokenListFilterInput) {
  turboTokenList(pagination: $pagination, filter: $filter) {
    meta {
      lastCursor
      firstCursor
      hasNextItems
      hasPreviousItems
    }
    items {
      id
      name
      symbol
      price
      initialPrice
      buysCount
      sellsCount
      uniqueTradersCount
      speedMode
      startDate
      endDate
      volumeUsdtDrops
    }
  }
}
'''

PAYLOAD = {
    'operationName': 'TurboTokenList',
    'variables': {
        'pagination': {
            'limit': 40
        },
        'filter': {}
    },
    'query': QUERY,
}

HEADERS = {
    'accept': '*/*',
    'content-type': 'application/json',
    'origin': 'https://catapult.trade',
    'referer': 'https://catapult.trade/turbo/discover',
    'user-agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/125.0.0.0 Safari/537.36'
    ),
}

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

session = requests.Session()


def save_raw(data):
    cur.execute(
        'INSERT INTO graphql_raw(ts, operation, response) VALUES (%s,%s,%s)',
        (datetime.utcnow(), 'TurboTokenList', json.dumps(data))
    )


def save_items(items):
    count = 0

    for item in items:
        cur.execute(
            '''
            INSERT INTO token_snapshots(
                ts, token_id, mode, price,
                volume, buys, sells, traders
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ''',
            (
                datetime.utcnow(),
                str(item.get('id')),
                item.get('speedMode'),
                float(item.get('price')) if item.get('price') else None,
                float(item.get('volumeUsdtDrops')) if item.get('volumeUsdtDrops') else 0,
                item.get('buysCount'),
                item.get('sellsCount'),
                item.get('uniqueTradersCount'),
            )
        )
        count += 1

    return count


print('direct graphql collector started', flush=True)

while True:
    try:
        r = session.post(
            GRAPHQL_URL,
            json=PAYLOAD,
            headers=HEADERS,
            timeout=30,
        )

        print(f'status={r.status_code}', flush=True)

        if r.status_code != 200:
            print(r.text[:500], flush=True)
            time.sleep(15)
            continue

        data = r.json()

        save_raw(data)

        items = (
            data
            .get('data', {})
            .get('turboTokenList', {})
            .get('items', [])
        )

        saved = save_items(items)

        print(f'direct snapshots saved: {saved}', flush=True)

    except Exception as exc:
        print(f'direct collector error: {exc}', flush=True)

    time.sleep(10)
