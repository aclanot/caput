import json
import os
from datetime import datetime
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


def load_ticks(value):
    if isinstance(value, str):
        return [float(x) for x in json.loads(value)]
    return [float(x) for x in value]


def short_result(ticks, entry_pump, tp_from_start, sl_from_start):
    start = ticks[0]
    entry_index = None
    for i, price in enumerate(ticks):
        if price >= start * entry_pump:
            entry_index = i
            break
    if entry_index is None:
        return None

    entry = ticks[entry_index]
    exit_price = ticks[-1]

    for price in ticks[entry_index + 1:]:
        if price <= start * tp_from_start:
            exit_price = price
            break
        if price >= start * sl_from_start:
            exit_price = price
            break

    return (entry / exit_price - 1) * 100 - FEE_PCT


def long_result(ticks, entry_dd, tp_from_entry, sl_from_entry):
    start = ticks[0]
    entry_index = None
    for i, price in enumerate(ticks):
        if price <= start * entry_dd:
            entry_index = i
            break
    if entry_index is None:
        return None

    entry = ticks[entry_index]
    exit_price = ticks[-1]

    for price in ticks[entry_index + 1:]:
        if price >= entry * tp_from_entry:
            exit_price = price
            break
        if price <= entry * sl_from_entry:
            exit_price = price
            break

    return (exit_price / entry - 1) * 100 - FEE_PCT


def save_result(run_ts, mode, side, strategy, entry, tp, sl, pnls):
    if len(pnls) < MIN_TRADES:
        return
    wins = [x for x in pnls if x > 0]
    avg = sum(pnls) / len(pnls)
    cur.execute('''
    INSERT INTO strategy_sweep(
        run_ts, mode, side, strategy, entry_threshold, take_profit, stop_loss,
        trades, winrate, avg_pnl, median_pnl, worst_pnl, best_pnl, expectancy
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', (
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

trajectories = []
for token_id, ticks_raw, mode in rows:
    try:
        ticks = load_ticks(ticks_raw)
        if len(ticks) >= 5:
            trajectories.append((str(token_id), mode or 'UNKNOWN', ticks))
    except Exception:
        pass

print(f'loaded trajectories: {len(trajectories)}', flush=True)

run_ts = datetime.utcnow()
modes = sorted(set(mode for _, mode, _ in trajectories))

short_entries = [1.10, 1.20, 1.30, 1.40, 1.50, 2.00]
short_tps = [0.95, 0.90, 0.80, 0.70, 0.50]
short_sls = [1.30, 1.50, 1.60, 2.00, 3.00]

long_entries = [0.50, 0.30, 0.20, 0.10, 0.05]
long_tps = [1.10, 1.20, 1.50, 2.00, 3.00]
long_sls = [0.80, 0.70, 0.50, 0.30]

for mode in modes:
    mode_ticks = [ticks for _, m, ticks in trajectories if m == mode]

    for entry in short_entries:
        for tp in short_tps:
            for sl in short_sls:
                if sl <= entry:
                    continue
                pnls = []
                for ticks in mode_ticks:
                    result = short_result(ticks, entry, tp, sl)
                    if result is not None:
                        pnls.append(result)
                strategy = f'short_pump_{int((entry-1)*100)}_tp_{int((1-tp)*100)}_sl_{int((sl-1)*100)}'
                save_result(run_ts, mode, 'SHORT', strategy, entry, tp, sl, pnls)

    for entry in long_entries:
        for tp in long_tps:
            for sl in long_sls:
                if sl >= 1 or tp <= 1:
                    continue
                pnls = []
                for ticks in mode_ticks:
                    result = long_result(ticks, entry, tp, sl)
                    if result is not None:
                        pnls.append(result)
                strategy = f'long_dd_{int((1-entry)*100)}_tp_{int((tp-1)*100)}_sl_{int((1-sl)*100)}'
                save_result(run_ts, mode, 'LONG', strategy, entry, tp, sl, pnls)

cur.execute('''
SELECT mode, side, strategy, trades, winrate, avg_pnl, median_pnl, worst_pnl, best_pnl
FROM strategy_sweep
ORDER BY avg_pnl DESC
LIMIT 20
''')

print('\nTOP SWEEP RESULTS\n', flush=True)
for r in cur.fetchall():
    print(
        f'{r[0]} {r[1]} {r[2]} | n={r[3]} | win={r[4]*100:.1f}% | avg={r[5]:.2f}% | med={r[6]:.2f}% | worst={r[7]:.2f}% | best={r[8]:.2f}%',
        flush=True,
    )

print(f'sweep completed at {run_ts} UTC', flush=True)
