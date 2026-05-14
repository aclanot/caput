import json
import os
from collections import defaultdict
from datetime import UTC, datetime
from statistics import median

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
MIN_TRADES = int(os.getenv('SWEEP_MIN_TRADES', '10'))
FEE_PCT = float(os.getenv('SWEEP_FEE_PCT', '0'))

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS strategy_sweep (
    id BIGSERIAL PRIMARY KEY,
    run_ts TIMESTAMP,
    mode TEXT,
    side TEXT,
    strategy TEXT,
    entry_threshold DOUBLE PRECISION,
    take_profit DOUBLE PRECISION,
    stop_loss DOUBLE PRECISION,
    trades INT,
    winrate DOUBLE PRECISION,
    avg_pnl DOUBLE PRECISION,
    median_pnl DOUBLE PRECISION,
    worst_pnl DOUBLE PRECISION,
    best_pnl DOUBLE PRECISION,
    expectancy DOUBLE PRECISION
)
''')

cur.execute('TRUNCATE TABLE strategy_sweep')

cur.execute('''
SELECT
    ft.token_id,
    ft.ticks,
    COALESCE(tf.mode, s.mode, 'UNKNOWN') AS mode
FROM finished_tokens ft
LEFT JOIN trajectory_features tf ON tf.token_id = ft.token_id
LEFT JOIN LATERAL (
    SELECT mode
    FROM token_snapshots s
    WHERE s.token_id = ft.token_id
      AND mode IS NOT NULL
    ORDER BY id DESC
    LIMIT 1
) s ON TRUE
''')
rows = cur.fetchall()

short_entries = [1.10, 1.20, 1.30, 1.40, 1.50, 2.00]
short_tps = [0.95, 0.90, 0.80, 0.70, 0.50]
short_sls = [1.30, 1.50, 1.60, 2.00, 3.00]

long_entries = [0.50, 0.30, 0.20, 0.10, 0.05]
long_tps = [1.10, 1.20, 1.50, 2.00, 3.00]
long_sls = [0.80, 0.70, 0.50, 0.30]


def load_ticks(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def strategy_key(mode, side, strategy, entry, tp, sl):
    return mode, side, strategy, entry, tp, sl


def pct_int(value):
    return int(round(value))


def first_entries_ge(values, thresholds):
    hits = {}
    remaining = sorted(thresholds)
    for index, value in enumerate(values):
        while remaining and value >= remaining[0]:
            hits[remaining.pop(0)] = index
        if not remaining:
            break
    return hits


def first_entries_le(values, thresholds):
    hits = {}
    remaining = sorted(thresholds, reverse=True)
    for index, value in enumerate(values):
        while remaining and value <= remaining[0]:
            hits[remaining.pop(0)] = index
        if not remaining:
            break
    return hits


def first_cross_after(values, start_index, ge_thresholds=(), le_thresholds=()):
    ge_hits = {}
    le_hits = {}
    ge_remaining = sorted(set(ge_thresholds))
    le_remaining = sorted(set(le_thresholds), reverse=True)

    for index in range(start_index, len(values)):
        value = values[index]
        while ge_remaining and value >= ge_remaining[0]:
            ge_hits[ge_remaining.pop(0)] = index
        while le_remaining and value <= le_remaining[0]:
            le_hits[le_remaining.pop(0)] = index
        if not ge_remaining and not le_remaining:
            break

    return ge_hits, le_hits


def add_result(results, mode, side, strategy, entry, tp, sl, pnl):
    results[strategy_key(mode, side, strategy, entry, tp, sl)].append(pnl)


def sweep_short(results, mode, rel_prices):
    entry_hits = first_entries_ge(rel_prices, short_entries)
    for entry in short_entries:
        entry_index = entry_hits.get(entry)
        if entry_index is None:
            continue

        entry_rel = rel_prices[entry_index]
        sl_hits, tp_hits = first_cross_after(
            rel_prices,
            entry_index + 1,
            ge_thresholds=[sl for sl in short_sls if sl > entry],
            le_thresholds=short_tps,
        )

        for tp in short_tps:
            for sl in short_sls:
                if sl <= entry:
                    continue

                tp_index = tp_hits.get(tp)
                sl_index = sl_hits.get(sl)
                if tp_index is None and sl_index is None:
                    exit_index = len(rel_prices) - 1
                elif sl_index is None or (tp_index is not None and tp_index <= sl_index):
                    exit_index = tp_index
                else:
                    exit_index = sl_index

                exit_rel = rel_prices[exit_index]
                if exit_rel <= 0:
                    continue
                pnl = (entry_rel / exit_rel - 1) * 100 - FEE_PCT
                strategy = (
                    f'short_pump_{pct_int((entry - 1) * 100)}_'
                    f'tp_{pct_int((1 - tp) * 100)}_'
                    f'sl_{pct_int((sl - 1) * 100)}'
                )
                add_result(results, mode, 'SHORT', strategy, entry, tp, sl, pnl)


def sweep_long(results, mode, rel_prices):
    entry_hits = first_entries_le(rel_prices, long_entries)
    for entry in long_entries:
        entry_index = entry_hits.get(entry)
        if entry_index is None:
            continue

        entry_rel = rel_prices[entry_index]
        ge_thresholds = [entry_rel * tp for tp in long_tps]
        le_thresholds = [entry_rel * sl for sl in long_sls]
        tp_hits, sl_hits = first_cross_after(
            rel_prices,
            entry_index + 1,
            ge_thresholds=ge_thresholds,
            le_thresholds=le_thresholds,
        )

        for tp in long_tps:
            tp_threshold = entry_rel * tp
            for sl in long_sls:
                if sl >= 1 or tp <= 1:
                    continue

                sl_threshold = entry_rel * sl
                tp_index = tp_hits.get(tp_threshold)
                sl_index = sl_hits.get(sl_threshold)
                if tp_index is None and sl_index is None:
                    exit_index = len(rel_prices) - 1
                elif sl_index is None or (tp_index is not None and tp_index <= sl_index):
                    exit_index = tp_index
                else:
                    exit_index = sl_index

                exit_rel = rel_prices[exit_index]
                pnl = (exit_rel / entry_rel - 1) * 100 - FEE_PCT
                strategy = (
                    f'long_dd_{pct_int((1 - entry) * 100)}_'
                    f'tp_{pct_int((tp - 1) * 100)}_'
                    f'sl_{pct_int((1 - sl) * 100)}'
                )
                add_result(results, mode, 'LONG', strategy, entry, tp, sl, pnl)


def normalize_prices(ticks):
    if not ticks or len(ticks) < 5:
        return None
    start = float(ticks[0])
    if start <= 0:
        return None
    return [float(price) / start for price in ticks]


run_ts = datetime.now(UTC).replace(tzinfo=None)
results = defaultdict(list)
processed = 0
failed = 0

for token_id, ticks_raw, mode in rows:
    try:
        rel_prices = normalize_prices(load_ticks(ticks_raw))
        if rel_prices is None:
            continue
        mode = mode or 'UNKNOWN'
        sweep_short(results, mode, rel_prices)
        sweep_long(results, mode, rel_prices)
        processed += 1
    except Exception as exc:
        failed += 1
        print(f'failed {token_id}: {exc}', flush=True)

insert_rows = []
for (mode, side, strategy, entry, tp, sl), pnls in results.items():
    if len(pnls) < MIN_TRADES:
        continue
    wins = [value for value in pnls if value > 0]
    avg = sum(pnls) / len(pnls)
    insert_rows.append((
        run_ts,
        mode,
        side,
        strategy,
        entry,
        tp,
        sl,
        len(pnls),
        len(wins) / len(pnls),
        avg,
        median(pnls),
        min(pnls),
        max(pnls),
        avg,
    ))

if insert_rows:
    cur.executemany('''
    INSERT INTO strategy_sweep(
        run_ts, mode, side, strategy, entry_threshold, take_profit, stop_loss,
        trades, winrate, avg_pnl, median_pnl, worst_pnl, best_pnl, expectancy
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', insert_rows)

print(
    f'processed trajectories: {processed} failed={failed} strategy_groups={len(results)} saved={len(insert_rows)}',
    flush=True,
)

cur.execute('''
SELECT mode, side, strategy, trades, winrate, avg_pnl, median_pnl, worst_pnl, best_pnl
FROM strategy_sweep
ORDER BY avg_pnl DESC, winrate DESC, trades DESC
LIMIT 20
''')

print('\nTOP SWEEP RESULTS\n', flush=True)
for row in cur.fetchall():
    print(
        f'{row[0]} {row[1]} {row[2]} | n={row[3]} | win={row[4]*100:.1f}% | '
        f'avg={row[5]:.2f}% | med={row[6]:.2f}% | worst={row[7]:.2f}% | best={row[8]:.2f}%',
        flush=True,
    )

cur.execute('''
SELECT side, COUNT(*)
FROM strategy_sweep
GROUP BY side
ORDER BY side
''')
print('\nSWEEP ROWS BY SIDE\n', flush=True)
for row in cur.fetchall():
    print(f'{row[0]}: {row[1]}', flush=True)

print(f'sweep completed at {run_ts} UTC', flush=True)
