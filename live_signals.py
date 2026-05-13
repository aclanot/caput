import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
MIN_SIGNAL_TRADES = int(os.getenv('LIVE_SIGNALS_MIN_TRADES', '20'))
MIN_SIGNAL_WINRATE = float(os.getenv('LIVE_SIGNALS_MIN_WINRATE', '0.50'))
MIN_SIGNAL_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_MIN_EXPECTANCY', '5'))
MAX_SIGNAL_AGE_SECONDS = float(os.getenv('LIVE_SIGNALS_MAX_AGE_SECONDS', '300'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS live_signals (
    id BIGSERIAL PRIMARY KEY,
    token_id TEXT,
    mode TEXT,
    side TEXT,
    signal_type TEXT,
    confidence TEXT,
    current_return_pct DOUBLE PRECISION,
    matched_strategy TEXT,
    historical_trades INT,
    historical_winrate DOUBLE PRECISION,
    historical_avg_pnl DOUBLE PRECISION,
    historical_median_pnl DOUBLE PRECISION,
    historical_worst_pnl DOUBLE PRECISION,
    reason TEXT,
    created_at TIMESTAMP
)
''')

cur.execute('DELETE FROM live_signals WHERE created_at < NOW() - interval \'1 day\'')

cur.execute('''
SELECT
    lf.token_id,
    lf.mode,
    lf.current_return_pct,
    lf.max_pump_pct,
    lf.max_drawdown_pct,
    lf.age_seconds,
    lf.volume,
    lf.buys,
    lf.sells,
    lf.traders,
    ss.side,
    ss.strategy,
    ss.entry_threshold,
    ss.take_profit,
    ss.stop_loss,
    ss.trades,
    ss.winrate,
    ss.avg_pnl,
    ss.median_pnl,
    ss.worst_pnl
FROM live_token_features lf
JOIN strategy_sweep ss
  ON ss.mode = lf.mode
WHERE lf.age_seconds <= %s
  AND ss.trades >= %s
  AND ss.winrate >= %s
  AND ss.avg_pnl >= %s
''', (
    MAX_SIGNAL_AGE_SECONDS,
    MIN_SIGNAL_TRADES,
    MIN_SIGNAL_WINRATE,
    MIN_SIGNAL_EXPECTANCY,
))

rows = cur.fetchall()
created = 0

for row in rows:
    (
        token_id,
        mode,
        current_return_pct,
        max_pump_pct,
        max_drawdown_pct,
        age_seconds,
        volume,
        buys,
        sells,
        traders,
        side,
        strategy,
        entry_threshold,
        take_profit,
        stop_loss,
        trades,
        winrate,
        avg_pnl,
        median_pnl,
        worst_pnl,
    ) = row

    matched = False

    if side == 'SHORT' and current_return_pct is not None:
        threshold_pct = (entry_threshold - 1.0) * 100.0
        if current_return_pct >= threshold_pct:
            matched = True

    if side == 'LONG' and current_return_pct is not None:
        drawdown_pct = (1.0 - entry_threshold) * 100.0
        if current_return_pct <= -drawdown_pct:
            matched = True

    if not matched:
        continue

    cur.execute('''
    SELECT 1
    FROM live_signals
    WHERE token_id = %s
      AND matched_strategy = %s
      AND created_at >= NOW() - interval '30 minutes'
    LIMIT 1
    ''', (token_id, strategy))

    if cur.fetchone():
        continue

    confidence = 'LOW'
    if winrate >= 0.60 and avg_pnl >= 15:
        confidence = 'MEDIUM'
    if winrate >= 0.65 and avg_pnl >= 25:
        confidence = 'HIGH'

    signal_type = f'{side}_WATCH'

    reason = (
        f'{mode} matched {strategy} | '
        f'current={current_return_pct:.2f}% | '
        f'winrate={winrate*100:.1f}% | '
        f'avg={avg_pnl:.2f}% | '
        f'median={median_pnl:.2f}% | '
        f'worst={worst_pnl:.2f}%'
    )

    cur.execute('''
    INSERT INTO live_signals(
        token_id, mode, side, signal_type, confidence,
        current_return_pct, matched_strategy,
        historical_trades, historical_winrate,
        historical_avg_pnl, historical_median_pnl,
        historical_worst_pnl,
        reason, created_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', (
        token_id,
        mode,
        side,
        signal_type,
        confidence,
        current_return_pct,
        strategy,
        trades,
        winrate,
        avg_pnl,
        median_pnl,
        worst_pnl,
        reason,
        datetime.utcnow(),
    ))

    created += 1

print(f'live signals created: {created}', flush=True)

cur.execute('''
SELECT
    token_id,
    mode,
    signal_type,
    confidence,
    current_return_pct,
    matched_strategy,
    historical_winrate,
    historical_avg_pnl
FROM live_signals
ORDER BY id DESC
LIMIT 20
''')

for row in cur.fetchall():
    print(
        f'{row[0]} {row[1]} {row[2]} {row[3]} current={row[4]:.2f}% strategy={row[5]} win={row[6]*100:.1f}% avg={row[7]:.2f}%',
        flush=True,
    )
