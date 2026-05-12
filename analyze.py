import json
import os
from datetime import datetime

import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
OUTPUT_DIR = os.getenv('ANALYZE_OUTPUT_DIR', 'analysis_output')

os.makedirs(OUTPUT_DIR, exist_ok=True)

conn = psycopg.connect(DATABASE_URL)

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

df = pd.read_sql(QUERY, conn)

print(f'loaded trajectories: {len(df)}', flush=True)

rows = []

for _, row in df.iterrows():
    try:
        token_id = row['token_id']
        mode = row['mode'] or 'UNKNOWN'
        ticks = row['ticks']

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

        roundtrip_after_90dd = False
        dead_bounce_20 = False
        pump30_dump20 = False

        hit_90dd_index = None
        hit_pump30_index = None

        for i, price in enumerate(ticks):
            rel = price / start

            if hit_90dd_index is None and rel <= 0.1:
                hit_90dd_index = i

            if hit_pump30_index is None and rel >= 1.3:
                hit_pump30_index = i

        if hit_90dd_index is not None:
            future = ticks[hit_90dd_index:]

            if max(future) >= start:
                roundtrip_after_90dd = True

            if max(future) >= start * 0.2:
                dead_bounce_20 = True

        if hit_pump30_index is not None:
            future = ticks[hit_pump30_index:]

            if min(future) <= start * 0.8:
                pump30_dump20 = True

        rows.append({
            'token_id': token_id,
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
        })

    except Exception as exc:
        print(f'failed processing token: {exc}', flush=True)

features_df = pd.DataFrame(rows)

if features_df.empty:
    print('no features extracted', flush=True)
    raise SystemExit(0)

features_path = os.path.join(OUTPUT_DIR, 'trajectory_features.csv')
features_df.to_csv(features_path, index=False)

print(f'saved features: {features_path}', flush=True)

summary = []

for mode, g in features_df.groupby('mode'):
    summary.append({
        'mode': mode,
        'n': len(g),
        'avg_final_return_pct': g['final_return_pct'].mean(),
        'avg_max_pump_pct': g['max_pump_pct'].mean(),
        'avg_max_drawdown_pct': g['max_drawdown_pct'].mean(),
        'p_final_below_start': (g['final_return_pct'] < 0).mean(),
        'p_roundtrip_after_90dd': g['roundtrip_after_90dd'].mean(),
        'p_dead_bounce_20': g['dead_bounce_20'].mean(),
        'p_pump30_dump20': g['pump30_dump20'].mean(),
    })

summary_df = pd.DataFrame(summary)
summary_path = os.path.join(OUTPUT_DIR, 'mode_summary.csv')
summary_df.to_csv(summary_path, index=False)

print('\nMODE SUMMARY\n', flush=True)
print(summary_df.to_string(index=False), flush=True)

print(f'\nsaved summary: {summary_path}', flush=True)
print(f'analysis completed at {datetime.utcnow()} UTC', flush=True)
