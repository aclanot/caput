import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
MIN_SIGNAL_TRADES = int(os.getenv('LIVE_SIGNALS_MIN_TRADES', '50'))
MIN_SIGNAL_WINRATE = float(os.getenv('LIVE_SIGNALS_MIN_WINRATE', '0.55'))
MIN_SIGNAL_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_MIN_EXPECTANCY', '5'))
MAX_SIGNAL_AGE_SECONDS = float(os.getenv('LIVE_SIGNALS_MAX_AGE_SECONDS', '300'))
MIN_SIGNAL_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_MIN_CONFIDENCE', '60'))
DEFAULT_SHORT_TP_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_TP_PCT', '30'))
DEFAULT_SHORT_SL_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_SL_PCT', '35'))
DEFAULT_LONG_TP_PCT = float(os.getenv('LIVE_SIGNAL_LONG_TP_PCT', '20'))
DEFAULT_LONG_SL_PCT = float(os.getenv('LIVE_SIGNAL_LONG_SL_PCT', '25'))

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
    confidence_pct INT,
    current_price DOUBLE PRECISION,
    entry_low DOUBLE PRECISION,
    entry_high DOUBLE PRECISION,
    take_profit_price DOUBLE PRECISION,
    stop_loss_price DOUBLE PRECISION,
    max_leverage DOUBLE PRECISION,
    current_return_pct DOUBLE PRECISION,
    matched_strategy TEXT,
    historical_trades INT,
    historical_winrate DOUBLE PRECISION,
    historical_avg_pnl DOUBLE PRECISION,
    historical_median_pnl DOUBLE PRECISION,
    historical_worst_pnl DOUBLE PRECISION,
    reason TEXT,
    sent_to_telegram BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP
)
''')

cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS confidence_pct INT')
cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS current_price DOUBLE PRECISION')
cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS entry_low DOUBLE PRECISION')
cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS entry_high DOUBLE PRECISION')
cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS take_profit_price DOUBLE PRECISION')
cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS stop_loss_price DOUBLE PRECISION')
cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS max_leverage DOUBLE PRECISION')
cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS sent_to_telegram BOOLEAN DEFAULT FALSE')

cur.execute('''
CREATE TABLE IF NOT EXISTS paper_signal_trades (
    id BIGSERIAL PRIMARY KEY,
    signal_id BIGINT UNIQUE,
    token_id TEXT,
    mode TEXT,
    side TEXT,
    status TEXT,
    confidence_pct INT,
    entry_price DOUBLE PRECISION,
    take_profit_price DOUBLE PRECISION,
    stop_loss_price DOUBLE PRECISION,
    max_leverage DOUBLE PRECISION,
    opened_at TIMESTAMP,
    closed_at TIMESTAMP,
    close_price DOUBLE PRECISION,
    pnl_pct DOUBLE PRECISION,
    close_reason TEXT
)
''')

cur.execute('DELETE FROM live_signals WHERE created_at < NOW() - interval \'1 day\'')


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def confidence_score(winrate, avg_pnl, median_pnl, worst_pnl, trades, mode, side):
    score = 40
    score += (winrate - 0.50) * 140
    score += min(max(avg_pnl, 0), 80) * 0.25
    score += min(max(median_pnl, -50), 80) * 0.15
    score += min(trades, 3000) / 3000 * 10

    if worst_pnl is not None and worst_pnl < -70:
        score -= 8
    if mode == 'CRACK' and side == 'SHORT':
        score += 7
    if mode == 'FLASH' and side == 'SHORT':
        score += 4
    if mode in ('MAYHEM', 'UNKNOWN'):
        score -= 5

    return int(round(clamp(score, 1, 99)))


def confidence_label(confidence_pct):
    if confidence_pct >= 80:
        return 'HIGH'
    if confidence_pct >= 65:
        return 'MEDIUM'
    return 'LOW'


def max_leverage_for(confidence_pct, mode, side):
    # Paper-only recommendation. Keep conservative until live robustness is proven.
    if side != 'SHORT':
        return 1.0
    if mode == 'CRACK' and confidence_pct >= 85:
        return 2.0
    if mode in ('CRACK', 'FLASH') and confidence_pct >= 75:
        return 1.5
    return 1.0


def prices_for_signal(side, current_price):
    if current_price is None or current_price <= 0:
        return None, None, None, None

    entry_low = current_price * 0.98
    entry_high = current_price * 1.02

    if side == 'SHORT':
        tp = current_price * (1.0 - DEFAULT_SHORT_TP_PCT / 100.0)
        sl = current_price * (1.0 + DEFAULT_SHORT_SL_PCT / 100.0)
    else:
        tp = current_price * (1.0 + DEFAULT_LONG_TP_PCT / 100.0)
        sl = current_price * (1.0 - DEFAULT_LONG_SL_PCT / 100.0)

    return entry_low, entry_high, tp, sl


cur.execute('''
SELECT
    lf.token_id,
    lf.mode,
    lf.current_price,
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
  AND ss.side = 'SHORT'
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
        current_price,
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

    if current_return_pct is None or current_price is None:
        continue

    threshold_pct = (entry_threshold - 1.0) * 100.0
    if current_return_pct < threshold_pct:
        continue

    conf_pct = confidence_score(winrate, avg_pnl, median_pnl, worst_pnl, trades, mode, side)
    if conf_pct < MIN_SIGNAL_CONFIDENCE:
        continue

    cur.execute('''
    SELECT 1
    FROM live_signals
    WHERE token_id = %s
      AND side = %s
      AND created_at >= NOW() - interval '30 minutes'
    LIMIT 1
    ''', (token_id, side))

    if cur.fetchone():
        continue

    entry_low, entry_high, tp_price, sl_price = prices_for_signal(side, current_price)
    max_lev = max_leverage_for(conf_pct, mode, side)
    conf_label = confidence_label(conf_pct)
    signal_type = f'{side}_SIGNAL'

    url = f'https://catapult.trade/ru/turbo/tokens/{token_id}'
    reason = (
        f'{mode} matched {strategy} | current={current_return_pct:.2f}% | '
        f'historical winrate={winrate*100:.1f}% | avg={avg_pnl:.2f}% | '
        f'median={median_pnl:.2f}% | worst={worst_pnl:.2f}% | '
        f'trades={trades} | url={url}'
    )

    cur.execute('''
    INSERT INTO live_signals(
        token_id, mode, side, signal_type, confidence, confidence_pct,
        current_price, entry_low, entry_high, take_profit_price, stop_loss_price, max_leverage,
        current_return_pct, matched_strategy,
        historical_trades, historical_winrate,
        historical_avg_pnl, historical_median_pnl,
        historical_worst_pnl,
        reason, created_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    RETURNING id
    ''', (
        token_id,
        mode,
        side,
        signal_type,
        conf_label,
        conf_pct,
        current_price,
        entry_low,
        entry_high,
        tp_price,
        sl_price,
        max_lev,
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
    signal_id = cur.fetchone()[0]

    cur.execute('''
    INSERT INTO paper_signal_trades(
        signal_id, token_id, mode, side, status, confidence_pct,
        entry_price, take_profit_price, stop_loss_price, max_leverage, opened_at
    ) VALUES (%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s)
    ON CONFLICT(signal_id) DO NOTHING
    ''', (
        signal_id,
        token_id,
        mode,
        side,
        conf_pct,
        current_price,
        tp_price,
        sl_price,
        max_lev,
        datetime.utcnow(),
    ))

    created += 1

print(f'live signals created: {created}', flush=True)

cur.execute('''
SELECT
    token_id,
    mode,
    side,
    confidence_pct,
    current_price,
    entry_low,
    entry_high,
    take_profit_price,
    stop_loss_price,
    max_leverage,
    matched_strategy,
    historical_winrate,
    historical_avg_pnl
FROM live_signals
ORDER BY id DESC
LIMIT 20
''')

for row in cur.fetchall():
    print(
        f'{row[0]} {row[1]} {row[2]} conf={row[3]}% price={row[4]:.6f} entry={row[5]:.6f}-{row[6]:.6f} tp={row[7]:.6f} sl={row[8]:.6f} lev=x{row[9]} strategy={row[10]} win={row[11]*100:.1f}% avg={row[12]:.2f}%',
        flush=True,
    )
