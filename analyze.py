import csv
import json
import os
from collections import defaultdict
from datetime import datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
OUTPUT_DIR = os.getenv('ANALYZE_OUTPUT_DIR', 'analysis_output')

os.makedirs(OUTPUT_DIR, exist_ok=True)

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS trajectory_features (
    token_id TEXT PRIMARY KEY,
    mode TEXT,
    ticks_count INT,
    start_price DOUBLE PRECISION,
    end_price DOUBLE PRECISION,
    max_price DOUBLE PRECISION,
    min_price DOUBLE PRECISION,
    final_return_pct DOUBLE PRECISION,
    max_pump_pct DOUBLE PRECISION,
    max_drawdown_pct DOUBLE PRECISION,
    roundtrip_after_90dd BOOLEAN,
    dead_bounce_20 BOOLEAN,
    pump30_dump20 BOOLEAN,
    updated_at TIMESTAMP
)
''')

QUERY = '''
SELECT
    ft.token_id,
    ft.ts,
    ft.ticks,
    ts.mode
FROM finished_tokens ft
LEFT JOIN LATERAL (
    SELECT mode
    FROM token_snapshots s
    WHERE s.token_id = ft.token_id
      AND mode IS NOT NULL
    ORDER BY s.id DESC
    LIMIT 1
) ts ON TRUE
'''

print('loading finished trajectories...', flush=True)
cur.execute(QUERY)
source_rows = cur.fetchall()
print(f'loaded trajectories: {len(source_rows)}', flush=True)

features = []

for token_id, ts, ticks, mode in source_rows:
    try:
        mode = mode or 'UNKNOWN'

        if isinstance(ticks, str):
            ticks = json.loads(ticks)

        if not ticks or len(ticks) < 5:
            continue

        ticks = [float(x) for x in ticks]

        start = ticks[0]
        end = ticks[-1]
        max_price = max(ticks)
        min_price = min(ticks)

        final_return_pct = (end / start - 1) * 100
        max_pump_pct = (max_price / start - 1) * 100
        max_drawdown_pct = (min_price / start - 1) * 100

        hit_90dd_index = None
        hit_pump30_index = None

        for i, price in enumerate(ticks):
            rel = price / start

            if hit_90dd_index is None and rel <= 0.1:
                hit_90dd_index = i

            if hit_pump30_index is None and rel >= 1.3:
                hit_pump30_index = i

        roundtrip_after_90dd = False
        dead_bounce_20 = False
        pump30_dump20 = False

        if hit_90dd_index is not None:
            future = ticks[hit_90dd_index:]
            roundtrip_after_90dd = max(future) >= start
            dead_bounce_20 = max(future) >= start * 0.2

        if hit_pump30_index is not None:
            future = ticks[hit_pump30_index:]
            pump30_dump20 = min(future) <= start * 0.8

        row = {
            'token_id': str(token_id),
            'mode': mode,
            'ticks_count': len(ticks),
            'start_price': start,
            'end_price': end,
            'max_price': max_price,
            'min_price': min_price,
            'final_return_pct': final_return_pct,
            'max_pump_pct': max_pump_pct,
            'max_drawdown_pct': max_drawdown_pct,
            'roundtrip_after_90dd': roundtrip_after_90dd,
            'dead_bounce_20': dead_bounce_20,
            'pump30_dump20': pump30_dump20,
        }
        features.append(row)

        cur.execute('''
        INSERT INTO trajectory_features(
            token_id, mode, ticks_count, start_price, end_price, max_price, min_price,
            final_return_pct, max_pump_pct, max_drawdown_pct,
            roundtrip_after_90dd, dead_bounce_20, pump30_dump20, updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (token_id) DO UPDATE SET
            mode = EXCLUDED.mode,
            ticks_count = EXCLUDED.ticks_count,
            start_price = EXCLUDED.start_price,
            end_price = EXCLUDED.end_price,
            max_price = EXCLUDED.max_price,
            min_price = EXCLUDED.min_price,
            final_return_pct = EXCLUDED.final_return_pct,
            max_pump_pct = EXCLUDED.max_pump_pct,
            max_drawdown_pct = EXCLUDED.max_drawdown_pct,
            roundtrip_after_90dd = EXCLUDED.roundtrip_after_90dd,
            dead_bounce_20 = EXCLUDED.dead_bounce_20,
            pump30_dump20 = EXCLUDED.pump30_dump20,
            updated_at = EXCLUDED.updated_at
        ''', (
            row['token_id'], row['mode'], row['ticks_count'], row['start_price'], row['end_price'],
            row['max_price'], row['min_price'], row['final_return_pct'], row['max_pump_pct'],
            row['max_drawdown_pct'], row['roundtrip_after_90dd'], row['dead_bounce_20'],
            row['pump30_dump20'], datetime.utcnow(),
        ))

    except Exception as exc:
        print(f'failed processing token {token_id}: {exc}', flush=True)

if not features:
    print('no features extracted', flush=True)
    raise SystemExit(0)

features_path = os.path.join(OUTPUT_DIR, 'trajectory_features.csv')
with open(features_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=list(features[0].keys()))
    writer.writeheader()
    writer.writerows(features)

print(f'saved features: {features_path}', flush=True)

summary = []
by_mode = defaultdict(list)
for row in features:
    by_mode[row['mode']].append(row)

for mode, rows in sorted(by_mode.items(), key=lambda item: len(item[1]), reverse=True):
    n = len(rows)

    def avg(key):
        return sum(float(r[key]) for r in rows) / n

    summary.append({
        'mode': mode,
        'n': n,
        'avg_final_return_pct': avg('final_return_pct'),
        'avg_max_pump_pct': avg('max_pump_pct'),
        'avg_max_drawdown_pct': avg('max_drawdown_pct'),
        'p_final_below_start': sum(1 for r in rows if r['final_return_pct'] < 0) / n,
        'p_roundtrip_after_90dd': sum(1 for r in rows if r['roundtrip_after_90dd']) / n,
        'p_dead_bounce_20': sum(1 for r in rows if r['dead_bounce_20']) / n,
        'p_pump30_dump20': sum(1 for r in rows if r['pump30_dump20']) / n,
    })

summary_path = os.path.join(OUTPUT_DIR, 'mode_summary.csv')
with open(summary_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
    writer.writeheader()
    writer.writerows(summary)

print('\nMODE SUMMARY\n', flush=True)
for row in summary:
    print(
        f"{row['mode']} | n={row['n']} | avg_final={row['avg_final_return_pct']:.2f}% | "
        f"avg_pump={row['avg_max_pump_pct']:.2f}% | avg_dd={row['avg_max_drawdown_pct']:.2f}% | "
        f"down={row['p_final_below_start']*100:.1f}% | "
        f"roundtrip90={row['p_roundtrip_after_90dd']*100:.1f}% | "
        f"bounce90={row['p_dead_bounce_20']*100:.1f}% | "
        f"pumpfail={row['p_pump30_dump20']*100:.1f}%",
        flush=True,
    )

print(f'\nsaved summary: {summary_path}', flush=True)
print(f'analysis completed at {datetime.utcnow()} UTC', flush=True)
