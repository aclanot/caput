import json
import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
FEE_PCT = float(os.getenv('PAPER_FEE_PCT', '0'))

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS paper_trades (
    id BIGSERIAL PRIMARY KEY,
    token_id TEXT,
    mode TEXT,
    strategy TEXT,
    side TEXT,
    entry_tick INT,
    exit_tick INT,
    entry_price DOUBLE PRECISION,
    exit_price DOUBLE PRECISION,
    gross_pnl_pct DOUBLE PRECISION,
    fee_pct DOUBLE PRECISION,
    pnl_pct DOUBLE PRECISION,
    result TEXT,
    created_at TIMESTAMP
)
''')

cur.execute('CREATE INDEX IF NOT EXISTS idx_paper_trades_strategy_mode ON paper_trades(strategy, mode)')
cur.execute('TRUNCATE TABLE paper_trades')

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
ORDER BY ft.ts DESC
''')
rows = cur.fetchall()


def load_ticks(value):
    if isinstance(value, str):
        return [float(x) for x in json.loads(value)]
    return [float(x) for x in value]


def insert_trade(token_id, mode, strategy, side, entry_tick, exit_tick, entry_price, exit_price, gross_pnl_pct, result):
    pnl_pct = gross_pnl_pct - FEE_PCT
    cur.execute('''
    INSERT INTO paper_trades(
        token_id, mode, strategy, side, entry_tick, exit_tick,
        entry_price, exit_price, gross_pnl_pct, fee_pct, pnl_pct, result, created_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', (
        str(token_id), mode, strategy, side, entry_tick, exit_tick,
        entry_price, exit_price, gross_pnl_pct, FEE_PCT, pnl_pct, result, datetime.utcnow()
    ))


def simulate_short_after_pump(token_id, mode, ticks, pump_mult, tp_mult_from_start, sl_mult_from_start):
    start = ticks[0]
    entry_tick = None

    for i, price in enumerate(ticks):
        if price >= start * pump_mult:
            entry_tick = i
            break

    if entry_tick is None:
        return

    entry_price = ticks[entry_tick]
    exit_tick = len(ticks) - 1
    exit_price = ticks[-1]
    result = 'expiry'

    for j in range(entry_tick + 1, len(ticks)):
        price = ticks[j]
        if price <= start * tp_mult_from_start:
            exit_tick = j
            exit_price = price
            result = 'take_profit'
            break
        if price >= start * sl_mult_from_start:
            exit_tick = j
            exit_price = price
            result = 'stop_loss'
            break

    gross_pnl_pct = (entry_price / exit_price - 1) * 100
    strategy = f'short_after_pump_{int((pump_mult-1)*100)}_tp_{int((1-tp_mult_from_start)*100)}_sl_{int((sl_mult_from_start-1)*100)}'
    insert_trade(token_id, mode, strategy, 'SHORT', entry_tick, exit_tick, entry_price, exit_price, gross_pnl_pct, result)


def simulate_long_after_drawdown(token_id, mode, ticks, dd_mult, tp_mult_from_entry, sl_mult_from_entry):
    start = ticks[0]
    entry_tick = None

    for i, price in enumerate(ticks):
        if price <= start * dd_mult:
            entry_tick = i
            break

    if entry_tick is None:
        return

    entry_price = ticks[entry_tick]
    exit_tick = len(ticks) - 1
    exit_price = ticks[-1]
    result = 'expiry'

    for j in range(entry_tick + 1, len(ticks)):
        price = ticks[j]
        if price >= entry_price * tp_mult_from_entry:
            exit_tick = j
            exit_price = price
            result = 'take_profit'
            break
        if price <= entry_price * sl_mult_from_entry:
            exit_tick = j
            exit_price = price
            result = 'stop_loss'
            break

    gross_pnl_pct = (exit_price / entry_price - 1) * 100
    strategy = f'long_after_dd_{int((1-dd_mult)*100)}_tp_{int((tp_mult_from_entry-1)*100)}_sl_{int((1-sl_mult_from_entry)*100)}'
    insert_trade(token_id, mode, strategy, 'LONG', entry_tick, exit_tick, entry_price, exit_price, gross_pnl_pct, result)


processed = 0

for token_id, ticks_raw, mode in rows:
    try:
        ticks = load_ticks(ticks_raw)
        if len(ticks) < 5:
            continue

        simulate_short_after_pump(token_id, mode, ticks, 1.20, 0.90, 1.50)
        simulate_short_after_pump(token_id, mode, ticks, 1.30, 0.80, 1.60)
        simulate_short_after_pump(token_id, mode, ticks, 1.50, 1.00, 2.00)

        simulate_long_after_drawdown(token_id, mode, ticks, 0.30, 1.20, 0.50)
        simulate_long_after_drawdown(token_id, mode, ticks, 0.10, 1.20, 0.50)
        simulate_long_after_drawdown(token_id, mode, ticks, 0.05, 2.00, 0.50)

        processed += 1
    except Exception as exc:
        print(f'failed {token_id}: {exc}', flush=True)

print(f'processed trajectories: {processed}', flush=True)

cur.execute('''
SELECT
    strategy,
    COALESCE(mode, 'UNKNOWN') AS mode,
    COUNT(*) AS n,
    AVG(pnl_pct) AS avg_pnl,
    AVG(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0.0 END) AS winrate,
    MIN(pnl_pct) AS worst,
    MAX(pnl_pct) AS best
FROM paper_trades
GROUP BY strategy, COALESCE(mode, 'UNKNOWN')
ORDER BY strategy, n DESC
''')
summary = cur.fetchall()

print('\nPAPER SUMMARY\n', flush=True)
for r in summary:
    print(
        f'{r[0]} / {r[1]} | n={r[2]} | avg={r[3]:.2f}% | winrate={r[4]*100:.1f}% | worst={r[5]:.2f}% | best={r[6]:.2f}%',
        flush=True,
    )

print(f'paper simulation completed at {datetime.utcnow()} UTC', flush=True)
