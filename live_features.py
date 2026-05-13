import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
MIN_SNAPSHOTS = int(os.getenv('LIVE_FEATURES_MIN_SNAPSHOTS', '2'))
LOOKBACK_MINUTES = int(os.getenv('LIVE_FEATURES_LOOKBACK_MINUTES', '30'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS live_token_features (
    token_id TEXT PRIMARY KEY,
    mode TEXT,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    age_seconds DOUBLE PRECISION,
    snapshots INT,
    initial_price DOUBLE PRECISION,
    current_price DOUBLE PRECISION,
    current_return_pct DOUBLE PRECISION,
    max_pump_pct DOUBLE PRECISION,
    max_drawdown_pct DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    buys INT,
    sells INT,
    traders INT,
    buy_sell_ratio DOUBLE PRECISION,
    updated_at TIMESTAMP
)
''')

cur.execute('''
WITH recent AS (
    SELECT *
    FROM token_snapshots
    WHERE ts >= NOW() - (%s || ' minutes')::interval
      AND token_id IS NOT NULL
      AND price IS NOT NULL
), grouped AS (
    SELECT
        token_id,
        COUNT(*) AS snapshots,
        MIN(ts) AS first_seen,
        MAX(ts) AS last_seen,
        (ARRAY_AGG(mode ORDER BY ts DESC))[1] AS mode,
        (ARRAY_AGG(price ORDER BY ts ASC))[1] AS initial_price,
        (ARRAY_AGG(price ORDER BY ts DESC))[1] AS current_price,
        MAX(price) AS max_price,
        MIN(price) AS min_price,
        (ARRAY_AGG(volume ORDER BY ts DESC))[1] AS volume,
        (ARRAY_AGG(buys ORDER BY ts DESC))[1] AS buys,
        (ARRAY_AGG(sells ORDER BY ts DESC))[1] AS sells,
        (ARRAY_AGG(traders ORDER BY ts DESC))[1] AS traders
    FROM recent
    GROUP BY token_id
)
INSERT INTO live_token_features(
    token_id, mode, first_seen, last_seen, age_seconds, snapshots,
    initial_price, current_price, current_return_pct, max_pump_pct, max_drawdown_pct,
    volume, buys, sells, traders, buy_sell_ratio, updated_at
)
SELECT
    token_id,
    COALESCE(mode, 'UNKNOWN') AS mode,
    first_seen,
    last_seen,
    EXTRACT(EPOCH FROM (last_seen - first_seen)) AS age_seconds,
    snapshots,
    initial_price,
    current_price,
    CASE WHEN initial_price > 0 THEN (current_price / initial_price - 1.0) * 100 ELSE NULL END,
    CASE WHEN initial_price > 0 THEN (max_price / initial_price - 1.0) * 100 ELSE NULL END,
    CASE WHEN initial_price > 0 THEN (min_price / initial_price - 1.0) * 100 ELSE NULL END,
    volume,
    buys,
    sells,
    traders,
    CASE
        WHEN COALESCE(sells, 0) = 0 THEN COALESCE(buys, 0)::DOUBLE PRECISION
        ELSE COALESCE(buys, 0)::DOUBLE PRECISION / NULLIF(sells, 0)
    END,
    %s
FROM grouped
WHERE snapshots >= %s
  AND initial_price IS NOT NULL
  AND current_price IS NOT NULL
  AND initial_price > 0
ON CONFLICT(token_id) DO UPDATE SET
    mode = EXCLUDED.mode,
    first_seen = EXCLUDED.first_seen,
    last_seen = EXCLUDED.last_seen,
    age_seconds = EXCLUDED.age_seconds,
    snapshots = EXCLUDED.snapshots,
    initial_price = EXCLUDED.initial_price,
    current_price = EXCLUDED.current_price,
    current_return_pct = EXCLUDED.current_return_pct,
    max_pump_pct = EXCLUDED.max_pump_pct,
    max_drawdown_pct = EXCLUDED.max_drawdown_pct,
    volume = EXCLUDED.volume,
    buys = EXCLUDED.buys,
    sells = EXCLUDED.sells,
    traders = EXCLUDED.traders,
    buy_sell_ratio = EXCLUDED.buy_sell_ratio,
    updated_at = EXCLUDED.updated_at
''', (LOOKBACK_MINUTES, datetime.utcnow(), MIN_SNAPSHOTS))

cur.execute('SELECT COUNT(*) FROM live_token_features WHERE updated_at >= NOW() - interval \'5 minutes\'')
updated = cur.fetchone()[0]

cur.execute('''
SELECT token_id, mode, snapshots, current_return_pct, max_pump_pct, max_drawdown_pct, buys, sells, traders
FROM live_token_features
WHERE updated_at >= NOW() - interval '5 minutes'
ORDER BY ABS(COALESCE(current_return_pct, 0)) DESC
LIMIT 10
''')
rows = cur.fetchall()

print(f'live features updated: {updated}', flush=True)
for row in rows:
    print(
        f'{row[0]} {row[1]} snaps={row[2]} current={row[3]:.2f}% pump={row[4]:.2f}% dd={row[5]:.2f}% buys={row[6]} sells={row[7]} traders={row[8]}',
        flush=True,
    )
