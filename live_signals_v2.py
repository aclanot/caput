import os
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
STALE_SECONDS = int(os.getenv('LIVE_SIGNALS_STALE_SECONDS', '15'))
MAX_AGE_SECONDS = float(os.getenv('LIVE_SIGNALS_MAX_AGE_SECONDS', '900'))
MIN_SNAPSHOTS = int(os.getenv('LIVE_SIGNALS_MIN_SNAPSHOTS', '3'))
MIN_TRADES = int(os.getenv('LIVE_SIGNALS_MIN_TRADES', '50'))
MIN_WINRATE = float(os.getenv('LIVE_SIGNALS_MIN_WINRATE', '0.55'))
MIN_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_MIN_EXPECTANCY', '5'))
MIN_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_MIN_CONFIDENCE', '65'))
LONG_MIN_TRADES = int(os.getenv('LIVE_SIGNALS_LONG_MIN_TRADES', '30'))
LONG_MIN_WINRATE = float(os.getenv('LIVE_SIGNALS_LONG_MIN_WINRATE', '0.70'))
LONG_MIN_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_LONG_MIN_EXPECTANCY', '0'))
LONG_MIN_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_LONG_MIN_CONFIDENCE', str(MIN_CONFIDENCE)))
MIN_PUMP = float(os.getenv('LIVE_SIGNALS_MIN_PUMP_FOR_REVERSAL_PCT', '50'))
MIN_REVERSAL = float(os.getenv('LIVE_SIGNALS_MIN_REVERSAL_FROM_PEAK_PCT', '12'))
SHORT_TP_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_TP_PCT', '30'))
SHORT_SL_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_SL_PCT', '35'))
LONG_TP_PCT = float(os.getenv('LIVE_SIGNAL_LONG_TP_PCT', '20'))
LONG_SL_PCT = float(os.getenv('LIVE_SIGNAL_LONG_SL_PCT', '25'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def ensure_schema():
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
    for ddl in [
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS live_snapshots INT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS max_pump_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reversal_from_peak_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_name TEXT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_risk TEXT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS adaptive_adjustment INT DEFAULT 0',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS adaptive_reason TEXT',
    ]:
        cur.execute(ddl)

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
    cur.execute('''
    CREATE TABLE IF NOT EXISTS signal_debug_log (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMP,
        token_id TEXT,
        mode TEXT,
        status TEXT,
        note TEXT
    )
    ''')


def classify_short_cluster(max_pump, current_return, buy_sell_ratio, snapshots):
    reversal = max_pump - current_return
    if max_pump >= 500 and reversal < 15:
        return 'mega_runner_live', 'DANGEROUS_SHORT'
    if max_pump >= 150 and reversal < 10:
        return 'strong_continuation_live', 'DANGEROUS_SHORT'
    if max_pump >= 50 and reversal >= 35:
        return 'pump_reversal_live', 'GOOD_SHORT'
    if max_pump >= 100 and reversal >= 20:
        return 'pump_exhaustion_live', 'GOOD_SHORT'
    if max_pump >= 50 and buy_sell_ratio is not None and buy_sell_ratio < 1.2 and snapshots >= 3:
        return 'weak_buy_pressure_live', 'OK_SHORT'
    return 'mixed_live', 'NEUTRAL'


def classify_long_cluster(current_return, max_drawdown, buy_sell_ratio, snapshots):
    drawdown = max(0, -(current_return or 0))
    max_drawdown_abs = max(0, -(max_drawdown or current_return or 0))
    bounce_from_low = (current_return - max_drawdown) if max_drawdown is not None else 0

    if drawdown >= 90 and (buy_sell_ratio is None or buy_sell_ratio < 0.8):
        return 'capitulation_falling_live', 'HIGH_RISK_LONG'
    if max_drawdown_abs >= 80 and bounce_from_low >= 10:
        return 'deep_bounce_live', 'GOOD_LONG'
    if drawdown >= 70 and buy_sell_ratio is not None and buy_sell_ratio >= 1.2 and snapshots >= 3:
        return 'deep_drawdown_buying_live', 'GOOD_LONG'
    if drawdown >= 50:
        return 'deep_drawdown_live', 'OK_LONG'
    return 'mixed_live', 'NEUTRAL'


def confidence(winrate, avg_pnl, median_pnl, worst_pnl, trades, mode, side, cluster_risk, setup_strength):
    score = 40
    score += (winrate - 0.50) * 140
    score += min(max(avg_pnl or 0, 0), 80) * 0.25
    score += min(max(median_pnl or 0, -50), 80) * 0.15
    score += min(trades or 0, 3000) / 3000 * 10
    score += min(max(setup_strength or 0, 0), 80) * 0.20

    if side == 'SHORT':
        if cluster_risk == 'GOOD_SHORT':
            score += 12
        elif cluster_risk == 'OK_SHORT':
            score += 5
        elif cluster_risk == 'DANGEROUS_SHORT':
            score -= 30
    elif side == 'LONG':
        if cluster_risk == 'GOOD_LONG':
            score += 10
        elif cluster_risk == 'OK_LONG':
            score += 5
        elif cluster_risk == 'HIGH_RISK_LONG':
            score -= 12

    if worst_pnl is not None and worst_pnl < -70:
        score -= 8
    if mode == 'CRACK' and side == 'SHORT':
        score += 7
    if mode == 'FLASH' and side == 'SHORT':
        score += 4
    if mode in ('MAYHEM', 'UNKNOWN'):
        score -= 5
    return int(round(clamp(score, 1, 99)))


def leverage_for(conf, mode, side):
    if side != 'SHORT':
        return 1.0
    if mode == 'CRACK' and conf >= 85:
        return 2.0
    if mode in ('CRACK', 'FLASH') and conf >= 75:
        return 1.5
    return 1.0


def prices_for_signal(side, current_price, current_return_pct=None, strategy_tp=None, strategy_sl=None):
    entry_low = current_price * 0.98
    entry_high = current_price * 1.02
    if side == 'SHORT':
        tp = None
        sl = None
        if current_return_pct is not None and current_return_pct > -99:
            initial_price = current_price / (1.0 + current_return_pct / 100.0)
            if strategy_tp and 0 < strategy_tp < 1:
                tp = initial_price * strategy_tp
            if strategy_sl and strategy_sl > 1:
                sl = initial_price * strategy_sl
        if tp is None or tp >= current_price:
            tp = current_price * (1.0 - SHORT_TP_PCT / 100.0)
        if sl is None or sl <= current_price:
            sl = current_price * (1.0 + SHORT_SL_PCT / 100.0)
    else:
        tp_mult = strategy_tp if strategy_tp and strategy_tp > 1 else 1.0 + LONG_TP_PCT / 100.0
        sl_mult = strategy_sl if strategy_sl and 0 < strategy_sl < 1 else 1.0 - LONG_SL_PCT / 100.0
        tp = current_price * tp_mult
        sl = current_price * sl_mult
    return entry_low, entry_high, tp, sl


def log_skip(token_id, mode, status, note):
    cur.execute(
        'INSERT INTO signal_debug_log(ts, token_id, mode, status, note) VALUES (%s,%s,%s,%s,%s)',
        (utcnow(), str(token_id), mode, status, note[:1000]),
    )


ensure_schema()
cur.execute('DELETE FROM live_signals WHERE created_at < NOW() - interval \'1 day\'')

cur.execute('''
SELECT COUNT(*) FROM live_token_features
WHERE last_seen >= NOW() - (%s || ' seconds')::interval
''', (STALE_SECONDS,))
fresh_live = cur.fetchone()[0]

cur.execute('''
SELECT UPPER(side), COUNT(*)
FROM strategy_sweep
WHERE trades >= %s
  AND winrate >= %s
  AND avg_pnl >= %s
  AND UPPER(side) = 'SHORT'
GROUP BY UPPER(side)
UNION ALL
SELECT UPPER(side), COUNT(*)
FROM strategy_sweep
WHERE trades >= %s
  AND winrate >= %s
  AND avg_pnl >= %s
  AND UPPER(side) = 'LONG'
GROUP BY UPPER(side)
''', (
    MIN_TRADES, MIN_WINRATE, MIN_EXPECTANCY,
    LONG_MIN_TRADES, LONG_MIN_WINRATE, LONG_MIN_EXPECTANCY,
))
strategy_counts = {side: count for side, count in cur.fetchall()}
strategy_count = sum(strategy_counts.values())

cur.execute('''
SELECT
    lf.token_id,
    UPPER(COALESCE(lf.mode, 'UNKNOWN')) AS mode,
    lf.current_price,
    lf.current_return_pct,
    lf.max_pump_pct,
    lf.max_drawdown_pct,
    lf.age_seconds,
    lf.snapshots,
    lf.buy_sell_ratio,
    UPPER(ss.side) AS side,
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
JOIN strategy_sweep ss ON UPPER(ss.mode) = UPPER(lf.mode)
WHERE lf.last_seen >= NOW() - (%s || ' seconds')::interval
  AND lf.age_seconds <= %s
  AND (
      (
          UPPER(ss.side) = 'SHORT'
          AND ss.trades >= %s
          AND ss.winrate >= %s
          AND ss.avg_pnl >= %s
      )
      OR (
          UPPER(ss.side) = 'LONG'
          AND ss.trades >= %s
          AND ss.winrate >= %s
          AND ss.avg_pnl >= %s
      )
  )
ORDER BY ss.avg_pnl DESC, ss.winrate DESC, ss.trades DESC, lf.last_seen DESC
''', (
    STALE_SECONDS, MAX_AGE_SECONDS,
    MIN_TRADES, MIN_WINRATE, MIN_EXPECTANCY,
    LONG_MIN_TRADES, LONG_MIN_WINRATE, LONG_MIN_EXPECTANCY,
))
rows = cur.fetchall()

print(
    f'live signal candidates: joined={len(rows)} fresh_live={fresh_live} strategies={strategy_count} '
    f'short_strategies={strategy_counts.get("SHORT", 0)} long_strategies={strategy_counts.get("LONG", 0)} '
    f'stale_limit={STALE_SECONDS}s',
    flush=True,
)

created = 0
skipped = {}

for row in rows:
    (
        token_id, mode, current_price, current_return, max_pump, max_dd, age, snapshots,
        bsr, side, strategy, entry_threshold, take_profit, stop_loss,
        trades, winrate, avg_pnl, med_pnl, worst_pnl,
    ) = row

    def skip(key, note=''):
        full_key = f'{side.lower()}_{key}'
        skipped[full_key] = skipped.get(full_key, 0) + 1
        if note:
            log_skip(token_id, mode, full_key, note)

    if current_price is None or current_return is None or max_pump is None:
        skip('missing_price')
        continue
    if snapshots < MIN_SNAPSHOTS:
        skip('too_few_snapshots')
        continue

    threshold_pct = (entry_threshold - 1.0) * 100.0
    if side == 'SHORT':
        setup_move = max_pump - current_return
        cluster_name, cluster_risk = classify_short_cluster(max_pump, current_return, bsr, snapshots)

        if cluster_risk == 'DANGEROUS_SHORT':
            skip('dangerous_runner_cluster', f'{cluster_name}; pump={max_pump:.2f}; reversal={setup_move:.2f}')
            continue
        if max_pump < MIN_PUMP:
            skip('pump_too_small')
            continue
        if setup_move < MIN_REVERSAL:
            skip('no_reversal_yet')
            continue
        if max_pump < threshold_pct:
            skip('strategy_threshold_not_met')
            continue
    elif side == 'LONG':
        if max_dd is None:
            skip('missing_drawdown')
            continue
        setup_move = max(0, -current_return)
        bounce_from_low = current_return - max_dd
        cluster_name, cluster_risk = classify_long_cluster(current_return, max_dd, bsr, snapshots)

        if current_return > threshold_pct:
            skip('strategy_threshold_not_met')
            continue
    else:
        skip('unsupported_side')
        continue

    conf = confidence(winrate, avg_pnl, med_pnl, worst_pnl, trades, mode, side, cluster_risk, setup_move)
    min_confidence = LONG_MIN_CONFIDENCE if side == 'LONG' else MIN_CONFIDENCE
    if conf < min_confidence:
        skip('low_confidence')
        continue

    cur.execute('''
    SELECT 1 FROM live_signals
    WHERE token_id = %s AND UPPER(side) = %s AND created_at >= NOW() - interval '30 minutes'
    LIMIT 1
    ''', (token_id, side))
    if cur.fetchone():
        skip('duplicate_recent')
        continue

    entry_low, entry_high, tp, sl = prices_for_signal(side, current_price, current_return, take_profit, stop_loss)
    lev = leverage_for(conf, mode, side)
    label = 'HIGH' if conf >= 80 else 'MEDIUM' if conf >= 65 else 'LOW'
    bounce_note = bounce_from_low if side == 'LONG' else setup_move
    reason = (
        f'{mode} {strategy} cluster={cluster_name}/{cluster_risk} '
        f'current={current_return:.2f}% max_pump={max_pump:.2f}% max_dd={max_dd:.2f}% '
        f'setup_move={setup_move:.2f}% bounce_from_low={bounce_note:.2f}% '
        f'snapshots={snapshots} historical_winrate={winrate*100:.1f}% avg={avg_pnl:.2f}% trades={trades}'
    )

    cur.execute('''
    INSERT INTO live_signals(
        token_id, mode, side, signal_type, confidence, confidence_pct,
        current_price, entry_low, entry_high, take_profit_price, stop_loss_price, max_leverage,
        current_return_pct, matched_strategy,
        historical_trades, historical_winrate, historical_avg_pnl, historical_median_pnl,
        historical_worst_pnl, reason, created_at,
        live_snapshots, max_pump_pct, reversal_from_peak_pct, cluster_name, cluster_risk,
        adaptive_adjustment, adaptive_reason
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,'v2_no_adaptive_yet')
    RETURNING id
    ''', (
        token_id, mode, side, f'{side}_SIGNAL', label, conf,
        current_price, entry_low, entry_high, tp, sl, lev,
        current_return, strategy,
        trades, winrate, avg_pnl, med_pnl, worst_pnl, reason, utcnow(),
        snapshots, max_pump, setup_move, cluster_name, cluster_risk,
    ))
    signal_id = cur.fetchone()[0]

    cur.execute('''
    INSERT INTO paper_signal_trades(
        signal_id, token_id, mode, side, status, confidence_pct,
        entry_price, take_profit_price, stop_loss_price, max_leverage, opened_at
    ) VALUES (%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s)
    ON CONFLICT(signal_id) DO NOTHING
    ''', (signal_id, token_id, mode, side, conf, current_price, tp, sl, lev, utcnow()))
    created += 1

print(f'live signals created: {created}', flush=True)
if skipped:
    print('signal skips:', ', '.join(f'{k}={v}' for k, v in sorted(skipped.items())), flush=True)
else:
    print('signal skips: none - no joined candidates or all candidates became signals', flush=True)

cur.execute('''
SELECT token_id, mode, side, confidence_pct, current_price, take_profit_price, stop_loss_price,
       matched_strategy, cluster_name, reversal_from_peak_pct
FROM live_signals
ORDER BY confidence_pct DESC NULLS LAST, historical_avg_pnl DESC NULLS LAST, id DESC
LIMIT 20
''')
for row in cur.fetchall():
    print(
        f'{row[0]} {row[1]} {row[2]} conf={row[3]}% price={row[4]:.6f} tp={row[5]:.6f} sl={row[6]:.6f} strategy={row[7]} cluster={row[8]} setup={row[9]:.2f}%',
        flush=True,
    )
