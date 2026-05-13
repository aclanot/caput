import os
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
MIN_CLOSED_TRADES = int(os.getenv('LIVE_PERFORMANCE_MIN_CLOSED_TRADES', '3'))
LOOKBACK_HOURS = int(os.getenv('LIVE_PERFORMANCE_LOOKBACK_HOURS', '24'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def table_exists(table_name):
    cur.execute('SELECT to_regclass(%s)', (f'public.{table_name}',))
    return cur.fetchone()[0] is not None


if not table_exists('paper_signal_trades'):
    print('paper_signal_trades table does not exist yet')
    raise SystemExit(0)

cur.execute('''
CREATE TABLE IF NOT EXISTS live_performance_summary (
    bucket_type TEXT,
    bucket_value TEXT,
    lookback_hours INT,
    closed_trades INT,
    winrate DOUBLE PRECISION,
    avg_pnl_pct DOUBLE PRECISION,
    median_pnl_pct DOUBLE PRECISION,
    worst_pnl_pct DOUBLE PRECISION,
    best_pnl_pct DOUBLE PRECISION,
    tp_count INT,
    sl_count INT,
    updated_at TIMESTAMP,
    PRIMARY KEY(bucket_type, bucket_value, lookback_hours)
)
''')

cur.execute('DELETE FROM live_performance_summary WHERE lookback_hours = %s', (LOOKBACK_HOURS,))

BUCKET_QUERIES = {
    'mode': '''
        SELECT COALESCE(pst.mode, 'UNKNOWN') AS bucket, pst.pnl_pct, pst.close_reason
        FROM paper_signal_trades pst
        WHERE pst.status = 'CLOSED'
          AND pst.closed_at >= NOW() - (%s || ' hours')::interval
          AND pst.pnl_pct IS NOT NULL
    ''',
    'cluster': '''
        SELECT COALESCE(ls.cluster_name, 'UNKNOWN') AS bucket, pst.pnl_pct, pst.close_reason
        FROM paper_signal_trades pst
        LEFT JOIN live_signals ls ON ls.id = pst.signal_id
        WHERE pst.status = 'CLOSED'
          AND pst.closed_at >= NOW() - (%s || ' hours')::interval
          AND pst.pnl_pct IS NOT NULL
    ''',
    'cluster_risk': '''
        SELECT COALESCE(ls.cluster_risk, 'UNKNOWN') AS bucket, pst.pnl_pct, pst.close_reason
        FROM paper_signal_trades pst
        LEFT JOIN live_signals ls ON ls.id = pst.signal_id
        WHERE pst.status = 'CLOSED'
          AND pst.closed_at >= NOW() - (%s || ' hours')::interval
          AND pst.pnl_pct IS NOT NULL
    ''',
    'confidence_bucket': '''
        SELECT
            CASE
                WHEN pst.confidence_pct >= 85 THEN '85-100'
                WHEN pst.confidence_pct >= 75 THEN '75-84'
                WHEN pst.confidence_pct >= 65 THEN '65-74'
                WHEN pst.confidence_pct >= 55 THEN '55-64'
                ELSE '0-54'
            END AS bucket,
            pst.pnl_pct,
            pst.close_reason
        FROM paper_signal_trades pst
        WHERE pst.status = 'CLOSED'
          AND pst.closed_at >= NOW() - (%s || ' hours')::interval
          AND pst.pnl_pct IS NOT NULL
    ''',
    'reversal_bucket': '''
        SELECT
            CASE
                WHEN COALESCE(ls.reversal_from_peak_pct, 0) >= 80 THEN '80+'
                WHEN COALESCE(ls.reversal_from_peak_pct, 0) >= 50 THEN '50-79'
                WHEN COALESCE(ls.reversal_from_peak_pct, 0) >= 30 THEN '30-49'
                WHEN COALESCE(ls.reversal_from_peak_pct, 0) >= 20 THEN '20-29'
                ELSE '0-19'
            END AS bucket,
            pst.pnl_pct,
            pst.close_reason
        FROM paper_signal_trades pst
        LEFT JOIN live_signals ls ON ls.id = pst.signal_id
        WHERE pst.status = 'CLOSED'
          AND pst.closed_at >= NOW() - (%s || ' hours')::interval
          AND pst.pnl_pct IS NOT NULL
    ''',
}


def median(values):
    values = sorted(values)
    n = len(values)
    if n == 0:
        return None
    mid = n // 2
    if n % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2


now = utcnow()
inserted = 0

for bucket_type, sql in BUCKET_QUERIES.items():
    cur.execute(sql, (LOOKBACK_HOURS,))
    rows = cur.fetchall()
    grouped = {}
    for bucket, pnl, close_reason in rows:
        grouped.setdefault(bucket, []).append((float(pnl), close_reason))

    for bucket, vals in grouped.items():
        n = len(vals)
        if n < MIN_CLOSED_TRADES:
            continue
        pnls = [v[0] for v in vals]
        wins = [p for p in pnls if p > 0]
        tp_count = sum(1 for _, r in vals if r == 'TP')
        sl_count = sum(1 for _, r in vals if r == 'SL')
        cur.execute('''
        INSERT INTO live_performance_summary(
            bucket_type, bucket_value, lookback_hours, closed_trades,
            winrate, avg_pnl_pct, median_pnl_pct, worst_pnl_pct, best_pnl_pct,
            tp_count, sl_count, updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(bucket_type, bucket_value, lookback_hours) DO UPDATE SET
            closed_trades = EXCLUDED.closed_trades,
            winrate = EXCLUDED.winrate,
            avg_pnl_pct = EXCLUDED.avg_pnl_pct,
            median_pnl_pct = EXCLUDED.median_pnl_pct,
            worst_pnl_pct = EXCLUDED.worst_pnl_pct,
            best_pnl_pct = EXCLUDED.best_pnl_pct,
            tp_count = EXCLUDED.tp_count,
            sl_count = EXCLUDED.sl_count,
            updated_at = EXCLUDED.updated_at
        ''', (
            bucket_type,
            str(bucket),
            LOOKBACK_HOURS,
            n,
            len(wins) / n,
            sum(pnls) / n,
            median(pnls),
            min(pnls),
            max(pnls),
            tp_count,
            sl_count,
            now,
        ))
        inserted += 1

cur.execute('''
SELECT COUNT(*) FROM paper_signal_trades
WHERE status = 'OPEN'
''')
open_trades = cur.fetchone()[0]

cur.execute('''
SELECT COUNT(*) FROM paper_signal_trades
WHERE status = 'CLOSED'
  AND closed_at >= NOW() - (%s || ' hours')::interval
''', (LOOKBACK_HOURS,))
closed_recent = cur.fetchone()[0]

print(f'live performance updated: buckets={inserted} open_trades={open_trades} closed_{LOOKBACK_HOURS}h={closed_recent}', flush=True)

cur.execute('''
SELECT bucket_type, bucket_value, closed_trades, winrate, avg_pnl_pct, median_pnl_pct, worst_pnl_pct, best_pnl_pct, tp_count, sl_count
FROM live_performance_summary
WHERE lookback_hours = %s
ORDER BY avg_pnl_pct DESC
LIMIT 20
''', (LOOKBACK_HOURS,))

rows = cur.fetchall()
if not rows:
    print('not enough closed live paper trades yet', flush=True)
else:
    print('\nTOP LIVE PERFORMANCE BUCKETS')
    for row in rows:
        bucket_type, bucket_value, n, winrate, avg, med, worst, best, tp, sl = row
        print(
            f'{bucket_type}={bucket_value} | n={n} win={winrate*100:.1f}% avg={avg:.2f}% med={med:.2f}% worst={worst:.2f}% best={best:.2f}% TP={tp} SL={sl}',
            flush=True,
        )
