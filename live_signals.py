import os
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
MIN_SIGNAL_TRADES = int(os.getenv('LIVE_SIGNALS_MIN_TRADES', '50'))
MIN_SIGNAL_WINRATE = float(os.getenv('LIVE_SIGNALS_MIN_WINRATE', '0.55'))
MIN_SIGNAL_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_MIN_EXPECTANCY', '5'))
LONG_MIN_SIGNAL_TRADES = int(os.getenv('LIVE_SIGNALS_LONG_MIN_TRADES', '30'))
LONG_MIN_SIGNAL_WINRATE = float(os.getenv('LIVE_SIGNALS_LONG_MIN_WINRATE', '0.70'))
LONG_MIN_SIGNAL_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_LONG_MIN_EXPECTANCY', '0'))
MAX_SIGNAL_AGE_SECONDS = float(os.getenv('LIVE_SIGNALS_MAX_AGE_SECONDS', '300'))
MIN_SIGNAL_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_MIN_CONFIDENCE', '60'))
LONG_MIN_SIGNAL_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_LONG_MIN_CONFIDENCE', str(MIN_SIGNAL_CONFIDENCE)))
MIN_REVERSAL_FROM_PEAK_PCT = float(os.getenv('LIVE_SIGNALS_MIN_REVERSAL_FROM_PEAK_PCT', '20'))
MIN_PUMP_FOR_REVERSAL_PCT = float(os.getenv('LIVE_SIGNALS_MIN_PUMP_FOR_REVERSAL_PCT', '50'))
MIN_LIVE_SNAPSHOTS_FOR_SIGNAL = int(os.getenv('LIVE_SIGNALS_MIN_SNAPSHOTS', '3'))
ALLOW_NO_REVERSAL_FOR_CRACK = os.getenv('LIVE_SIGNALS_ALLOW_NO_REVERSAL_FOR_CRACK', 'false').lower() in ('1', 'true', 'yes', 'on')
DEFAULT_SHORT_TP_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_TP_PCT', '30'))
DEFAULT_SHORT_SL_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_SL_PCT', '35'))
DEFAULT_LONG_TP_PCT = float(os.getenv('LIVE_SIGNAL_LONG_TP_PCT', '20'))
DEFAULT_LONG_SL_PCT = float(os.getenv('LIVE_SIGNAL_LONG_SL_PCT', '25'))
ADAPTIVE_LIVE_ENABLED = os.getenv('LIVE_SIGNALS_ADAPTIVE_LIVE_ENABLED', 'true').lower() in ('1', 'true', 'yes', 'on')
ADAPTIVE_MIN_CLOSED_TRADES = int(os.getenv('LIVE_SIGNALS_ADAPTIVE_MIN_CLOSED_TRADES', '5'))
ADAPTIVE_DISABLE_WINRATE = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_DISABLE_WINRATE', '0.35'))
ADAPTIVE_DISABLE_AVG_PNL = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_DISABLE_AVG_PNL', '-15'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


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
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS confidence_pct INT',
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS current_price DOUBLE PRECISION',
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS entry_low DOUBLE PRECISION',
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS entry_high DOUBLE PRECISION',
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS take_profit_price DOUBLE PRECISION',
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS stop_loss_price DOUBLE PRECISION',
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS max_leverage DOUBLE PRECISION',
    'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS sent_to_telegram BOOLEAN DEFAULT FALSE',
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

cur.execute('DELETE FROM live_signals WHERE created_at < NOW() - interval \'1 day\'')


def table_exists(table_name):
    cur.execute('SELECT to_regclass(%s)', (f'public.{table_name}',))
    return cur.fetchone()[0] is not None


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def classify_live_cluster(current_return_pct, max_pump_pct, max_drawdown_pct, reversal_from_peak_pct, buy_sell_ratio, snapshots):
    if max_pump_pct is None:
        return 'unknown_live', 'UNKNOWN'
    if max_pump_pct >= 500 and reversal_from_peak_pct < 15:
        return 'mega_runner_live', 'DANGEROUS_SHORT'
    if max_pump_pct >= 150 and reversal_from_peak_pct < 10:
        return 'strong_continuation_live', 'DANGEROUS_SHORT'
    if max_pump_pct >= 50 and reversal_from_peak_pct >= 35:
        return 'pump_reversal_live', 'GOOD_SHORT'
    if max_pump_pct >= 100 and reversal_from_peak_pct >= 20:
        return 'pump_exhaustion_live', 'GOOD_SHORT'
    if max_pump_pct >= 50 and buy_sell_ratio is not None and buy_sell_ratio < 1.2 and snapshots >= 3:
        return 'weak_buy_pressure_live', 'OK_SHORT'
    return 'mixed_live', 'NEUTRAL'


def classify_long_cluster(current_return_pct, max_drawdown_pct, buy_sell_ratio, snapshots):
    drawdown = max(0, -(current_return_pct or 0))
    max_drawdown_abs = max(0, -(max_drawdown_pct or current_return_pct or 0))
    bounce_from_low = (current_return_pct - max_drawdown_pct) if max_drawdown_pct is not None else 0

    if drawdown >= 90 and (buy_sell_ratio is None or buy_sell_ratio < 0.8):
        return 'capitulation_falling_live', 'HIGH_RISK_LONG'
    if max_drawdown_abs >= 80 and bounce_from_low >= 10:
        return 'deep_bounce_live', 'GOOD_LONG'
    if drawdown >= 70 and buy_sell_ratio is not None and buy_sell_ratio >= 1.2 and snapshots >= 3:
        return 'deep_drawdown_buying_live', 'GOOD_LONG'
    if drawdown >= 50:
        return 'deep_drawdown_live', 'OK_LONG'
    return 'mixed_live', 'NEUTRAL'


def cluster_adjustment(cluster_name, cluster_risk):
    if cluster_risk == 'GOOD_SHORT':
        return 12
    if cluster_risk == 'OK_SHORT':
        return 5
    if cluster_risk == 'DANGEROUS_SHORT':
        return -30
    if cluster_risk == 'GOOD_LONG':
        return 10
    if cluster_risk == 'OK_LONG':
        return 5
    if cluster_risk == 'HIGH_RISK_LONG':
        return -12
    return 0


def live_performance_adjustment(mode, cluster_name, cluster_risk):
    if not ADAPTIVE_LIVE_ENABLED or not table_exists('live_performance_summary'):
        return 0, 'adaptive_disabled_or_no_summary'

    checks = [
        ('cluster', cluster_name),
        ('cluster_risk', cluster_risk),
        ('mode', mode),
    ]
    total_adj = 0
    reasons = []

    for bucket_type, bucket_value in checks:
        if not bucket_value:
            continue
        cur.execute('''
        SELECT closed_trades, winrate, avg_pnl_pct, median_pnl_pct
        FROM live_performance_summary
        WHERE bucket_type = %s
          AND bucket_value = %s
        ORDER BY lookback_hours ASC
        LIMIT 1
        ''', (bucket_type, str(bucket_value)))
        row = cur.fetchone()
        if not row:
            continue

        closed, winrate, avg_pnl, median_pnl = row
        if closed < ADAPTIVE_MIN_CLOSED_TRADES:
            reasons.append(f'{bucket_type}:{bucket_value}=not_enough_live_n{closed}')
            continue

        adj = 0
        if winrate <= ADAPTIVE_DISABLE_WINRATE and avg_pnl <= ADAPTIVE_DISABLE_AVG_PNL:
            adj = -100
        elif winrate < 0.45 or avg_pnl < -5:
            adj = -15
        elif winrate >= 0.65 and avg_pnl > 10:
            adj = 12
        elif winrate >= 0.55 and avg_pnl > 0:
            adj = 5

        total_adj += adj
        reasons.append(f'{bucket_type}:{bucket_value} n={closed} win={winrate*100:.1f}% avg={avg_pnl:.1f}% adj={adj}')

    return int(clamp(total_adj, -100, 25)), '; '.join(reasons) if reasons else 'no_live_performance_match'


def confidence_score(winrate, avg_pnl, median_pnl, worst_pnl, trades, mode, side, cluster_name, cluster_risk, reversal_from_peak_pct):
    score = 40
    score += (winrate - 0.50) * 140
    score += min(max(avg_pnl or 0, 0), 80) * 0.25
    score += min(max(median_pnl or 0, -50), 80) * 0.15
    score += min(trades or 0, 3000) / 3000 * 10
    score += min(max(reversal_from_peak_pct or 0, 0), 80) * 0.20
    score += cluster_adjustment(cluster_name, cluster_risk)

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


def max_leverage_for(confidence_pct, mode, side, cluster_risk):
    if side != 'SHORT':
        return 1.0
    if cluster_risk == 'DANGEROUS_SHORT':
        return 0.0
    if mode == 'CRACK' and confidence_pct >= 85:
        return 2.0
    if mode in ('CRACK', 'FLASH') and confidence_pct >= 75:
        return 1.5
    return 1.0


def prices_for_signal(side, current_price, current_return_pct=None, strategy_tp=None, strategy_sl=None):
    if current_price is None or current_price <= 0:
        return None, None, None, None
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
            tp = current_price * (1.0 - DEFAULT_SHORT_TP_PCT / 100.0)
        if sl is None or sl <= current_price:
            sl = current_price * (1.0 + DEFAULT_SHORT_SL_PCT / 100.0)
    else:
        tp_mult = strategy_tp if strategy_tp and strategy_tp > 1 else 1.0 + DEFAULT_LONG_TP_PCT / 100.0
        sl_mult = strategy_sl if strategy_sl and 0 < strategy_sl < 1 else 1.0 - DEFAULT_LONG_SL_PCT / 100.0
        tp = current_price * tp_mult
        sl = current_price * sl_mult
    return entry_low, entry_high, tp, sl


def log_skip(token_id, mode, status, note):
    cur.execute(
        'INSERT INTO signal_debug_log(ts, token_id, mode, status, note) VALUES (%s,%s,%s,%s,%s)',
        (utcnow(), str(token_id), mode, status, note[:1000]),
    )


cur.execute('''
SELECT
    lf.token_id, lf.mode, lf.current_price, lf.current_return_pct,
    lf.max_pump_pct, lf.max_drawdown_pct, lf.age_seconds,
    lf.snapshots, lf.volume, lf.buys, lf.sells, lf.traders, lf.buy_sell_ratio,
    ss.side, ss.strategy, ss.entry_threshold, ss.take_profit, ss.stop_loss,
    ss.trades, ss.winrate, ss.avg_pnl, ss.median_pnl, ss.worst_pnl
FROM live_token_features lf
JOIN strategy_sweep ss ON UPPER(ss.mode) = UPPER(lf.mode)
WHERE lf.age_seconds <= %s
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
ORDER BY ss.avg_pnl DESC, ss.winrate DESC, ss.trades DESC
''', (
    MAX_SIGNAL_AGE_SECONDS,
    MIN_SIGNAL_TRADES, MIN_SIGNAL_WINRATE, MIN_SIGNAL_EXPECTANCY,
    LONG_MIN_SIGNAL_TRADES, LONG_MIN_SIGNAL_WINRATE, LONG_MIN_SIGNAL_EXPECTANCY,
))

rows = cur.fetchall()
created = 0
skipped = {}

for row in rows:
    (
        token_id, mode, current_price, current_return_pct, max_pump_pct, max_drawdown_pct,
        age_seconds, snapshots, volume, buys, sells, traders, buy_sell_ratio,
        side, strategy, entry_threshold, take_profit, stop_loss,
        trades, winrate, avg_pnl, median_pnl, worst_pnl,
    ) = row
    side = (side or '').upper()
    mode = (mode or 'UNKNOWN').upper()

    if current_return_pct is None or current_price is None or max_pump_pct is None:
        skipped['missing_price'] = skipped.get('missing_price', 0) + 1
        continue

    if snapshots < MIN_LIVE_SNAPSHOTS_FOR_SIGNAL:
        skipped['too_few_snapshots'] = skipped.get('too_few_snapshots', 0) + 1
        continue

    threshold_pct = (entry_threshold - 1.0) * 100.0
    if side == 'SHORT':
        setup_move_pct = max_pump_pct - current_return_pct
        cluster_name, cluster_risk = classify_live_cluster(
            current_return_pct, max_pump_pct, max_drawdown_pct, setup_move_pct, buy_sell_ratio, snapshots
        )

        if cluster_risk == 'DANGEROUS_SHORT':
            skipped['short_dangerous_runner_cluster'] = skipped.get('short_dangerous_runner_cluster', 0) + 1
            log_skip(token_id, mode, 'skip_runner', f'{cluster_name}; pump={max_pump_pct:.2f}; reversal={setup_move_pct:.2f}')
            continue

        if max_pump_pct < MIN_PUMP_FOR_REVERSAL_PCT:
            skipped['short_pump_too_small'] = skipped.get('short_pump_too_small', 0) + 1
            continue

        if setup_move_pct < MIN_REVERSAL_FROM_PEAK_PCT:
            if not (ALLOW_NO_REVERSAL_FOR_CRACK and mode == 'CRACK'):
                skipped['short_no_reversal_yet'] = skipped.get('short_no_reversal_yet', 0) + 1
                continue

        if max_pump_pct < threshold_pct:
            skipped['short_strategy_threshold_not_met'] = skipped.get('short_strategy_threshold_not_met', 0) + 1
            continue
    elif side == 'LONG':
        if max_drawdown_pct is None:
            skipped['long_missing_drawdown'] = skipped.get('long_missing_drawdown', 0) + 1
            continue

        setup_move_pct = max(0, -current_return_pct)
        cluster_name, cluster_risk = classify_long_cluster(current_return_pct, max_drawdown_pct, buy_sell_ratio, snapshots)

        if current_return_pct > threshold_pct:
            skipped['long_strategy_threshold_not_met'] = skipped.get('long_strategy_threshold_not_met', 0) + 1
            continue
    else:
        skipped['unsupported_side'] = skipped.get('unsupported_side', 0) + 1
        continue

    base_conf = confidence_score(winrate, avg_pnl, median_pnl, worst_pnl, trades, mode, side, cluster_name, cluster_risk, setup_move_pct)
    adaptive_adj, adaptive_reason = live_performance_adjustment(mode, cluster_name, cluster_risk)
    if adaptive_adj <= -100:
        skipped['adaptive_disabled_setup'] = skipped.get('adaptive_disabled_setup', 0) + 1
        log_skip(token_id, mode, 'adaptive_disabled', adaptive_reason)
        continue

    conf_pct = int(round(clamp(base_conf + adaptive_adj, 1, 99)))
    min_confidence = LONG_MIN_SIGNAL_CONFIDENCE if side == 'LONG' else MIN_SIGNAL_CONFIDENCE
    if conf_pct < min_confidence:
        skipped['low_confidence'] = skipped.get('low_confidence', 0) + 1
        continue

    cur.execute('''
    SELECT 1 FROM live_signals
    WHERE token_id = %s AND UPPER(side) = %s AND created_at >= NOW() - interval '30 minutes'
    LIMIT 1
    ''', (token_id, side))
    if cur.fetchone():
        skipped['duplicate_recent'] = skipped.get('duplicate_recent', 0) + 1
        continue

    entry_low, entry_high, tp_price, sl_price = prices_for_signal(
        side, current_price, current_return_pct, take_profit, stop_loss
    )
    max_lev = max_leverage_for(conf_pct, mode, side, cluster_risk)
    if max_lev <= 0:
        skipped['zero_leverage'] = skipped.get('zero_leverage', 0) + 1
        continue

    conf_label = confidence_label(conf_pct)
    signal_type = f'{side}_SIGNAL'
    url = f'https://catapult.trade/ru/turbo/tokens/{token_id}'
    reason = (
        f'{mode} matched {strategy} | cluster={cluster_name}/{cluster_risk} | '
        f'base_conf={base_conf}% adaptive_adj={adaptive_adj} | adaptive={adaptive_reason} | '
        f'current={current_return_pct:.2f}% | max_pump={max_pump_pct:.2f}% | '
        f'setup_move={setup_move_pct:.2f}% | snapshots={snapshots} | '
        f'historical winrate={winrate*100:.1f}% | avg={avg_pnl:.2f}% | '
        f'median={median_pnl:.2f}% | worst={worst_pnl:.2f}% | trades={trades} | url={url}'
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
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    RETURNING id
    ''', (
        token_id, mode, side, signal_type, conf_label, conf_pct,
        current_price, entry_low, entry_high, tp_price, sl_price, max_lev,
        current_return_pct, strategy,
        trades, winrate, avg_pnl, median_pnl, worst_pnl, reason, utcnow(),
        snapshots, max_pump_pct, setup_move_pct, cluster_name, cluster_risk,
        adaptive_adj, adaptive_reason,
    ))
    signal_id = cur.fetchone()[0]

    cur.execute('''
    INSERT INTO paper_signal_trades(
        signal_id, token_id, mode, side, status, confidence_pct,
        entry_price, take_profit_price, stop_loss_price, max_leverage, opened_at
    ) VALUES (%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s)
    ON CONFLICT(signal_id) DO NOTHING
    ''', (signal_id, token_id, mode, side, conf_pct, current_price, tp_price, sl_price, max_lev, utcnow()))
    created += 1

print(f'live signals created: {created}', flush=True)
if skipped:
    print('signal skips:', ', '.join(f'{k}={v}' for k, v in sorted(skipped.items())), flush=True)

cur.execute('''
SELECT token_id, mode, side, confidence_pct, current_price, entry_low, entry_high,
       take_profit_price, stop_loss_price, max_leverage, matched_strategy,
       historical_winrate, historical_avg_pnl, cluster_name, reversal_from_peak_pct,
       adaptive_adjustment
FROM live_signals
ORDER BY confidence_pct DESC NULLS LAST, historical_avg_pnl DESC NULLS LAST, id DESC
LIMIT 20
''')

for row in cur.fetchall():
    print(
        f'{row[0]} {row[1]} {row[2]} conf={row[3]}% price={row[4]:.6f} '
        f'entry={row[5]:.6f}-{row[6]:.6f} tp={row[7]:.6f} sl={row[8]:.6f} '
        f'lev=x{row[9]} strategy={row[10]} win={row[11]*100:.1f}% avg={row[12]:.2f}% '
        f'cluster={row[13]} setup={row[14]:.2f}% adaptive={row[15]}',
        flush=True,
    )
