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
MIN_PUMP = float(os.getenv('LIVE_SIGNALS_MIN_PUMP_FOR_REVERSAL_PCT', '50'))
MIN_REVERSAL = float(os.getenv('LIVE_SIGNALS_MIN_REVERSAL_FROM_PEAK_PCT', '12'))
ALLOW_NO_REVERSAL_FOR_CRACK = os.getenv('LIVE_SIGNALS_ALLOW_NO_REVERSAL_FOR_CRACK', 'false').lower() in ('1', 'true', 'yes', 'on')
SHORT_TP_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_TP_PCT', '30'))
SHORT_SL_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_SL_PCT', '35'))
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


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def table_exists(name):
    cur.execute('SELECT to_regclass(%s)', (f'public.{name}',))
    return cur.fetchone()[0] is not None


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


def log_skip(token_id, mode, status, note):
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
    cur.execute(
        'INSERT INTO signal_debug_log(ts, token_id, mode, status, note) VALUES (%s,%s,%s,%s,%s)',
        (utcnow(), str(token_id), mode, status, note[:1000]),
    )


def classify_cluster(max_pump, reversal, buy_sell_ratio, snapshots):
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


def adaptive_adjustment(mode, cluster_name, cluster_risk):
    if not ADAPTIVE_LIVE_ENABLED or not table_exists('live_performance_summary'):
        return 0, 'adaptive_disabled_or_no_summary'

    total_adj = 0
    reasons = []
    for bucket_type, bucket_value in [('cluster', cluster_name), ('cluster_risk', cluster_risk), ('mode', mode)]:
        if not bucket_value:
            continue
        cur.execute('''
        SELECT closed_trades, winrate, avg_pnl_pct
        FROM live_performance_summary
        WHERE bucket_type = %s AND bucket_value = %s
        ORDER BY lookback_hours ASC
        LIMIT 1
        ''', (bucket_type, str(bucket_value)))
        row = cur.fetchone()
        if not row:
            continue
        closed, winrate, avg_pnl = row
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


def confidence_score(winrate, avg_pnl, median_pnl, worst_pnl, trades, mode, cluster_risk, reversal):
    score = 40
    score += (winrate - 0.50) * 140
    score += min(max(avg_pnl or 0, 0), 80) * 0.25
    score += min(max(median_pnl or 0, -50), 80) * 0.15
    score += min(trades or 0, 3000) / 3000 * 10
    score += min(max(reversal or 0, 0), 80) * 0.20
    if cluster_risk == 'GOOD_SHORT':
        score += 12
    elif cluster_risk == 'OK_SHORT':
        score += 5
    elif cluster_risk == 'DANGEROUS_SHORT':
        score -= 30
    if worst_pnl is not None and worst_pnl < -70:
        score -= 8
    if mode == 'CRACK':
        score += 7
    if mode == 'FLASH':
        score += 4
    if mode in ('MAYHEM', 'UNKNOWN'):
        score -= 5
    return int(round(clamp(score, 1, 99)))


def confidence_label(conf):
    return 'HIGH' if conf >= 80 else ('MEDIUM' if conf >= 65 else 'LOW')


ensure_schema()
cur.execute("DELETE FROM live_signals WHERE created_at < NOW() - interval '1 day'")

cur.execute('''SELECT COUNT(*) FROM live_token_features WHERE last_seen >= NOW() - (%s || ' seconds')::interval''', (STALE_SECONDS,))
fresh_live = cur.fetchone()[0]
cur.execute('''SELECT COUNT(*) FROM strategy_sweep WHERE trades >= %s AND winrate >= %s AND avg_pnl >= %s AND UPPER(side)='SHORT' ''', (MIN_TRADES, MIN_WINRATE, MIN_EXPECTANCY))
eligible_strategies = cur.fetchone()[0]

cur.execute('''
SELECT lf.token_id, UPPER(COALESCE(lf.mode, 'UNKNOWN')) AS mode,
       lf.current_price, lf.current_return_pct, lf.max_pump_pct, lf.snapshots, lf.buy_sell_ratio,
       ss.strategy, ss.entry_threshold, ss.trades, ss.winrate, ss.avg_pnl, ss.median_pnl, ss.worst_pnl
FROM live_token_features lf
JOIN strategy_sweep ss ON UPPER(ss.mode) = UPPER(lf.mode)
WHERE lf.last_seen >= NOW() - (%s || ' seconds')::interval
  AND lf.age_seconds <= %s
  AND ss.trades >= %s
  AND ss.winrate >= %s
  AND ss.avg_pnl >= %s
  AND UPPER(ss.side) = 'SHORT'
ORDER BY ss.avg_pnl DESC
''', (STALE_SECONDS, MAX_AGE_SECONDS, MIN_TRADES, MIN_WINRATE, MIN_EXPECTANCY))
rows = cur.fetchall()

print(f'live signal inputs: fresh_live={fresh_live} eligible_strategies={eligible_strategies} joined_candidates={len(rows)} stale_limit={STALE_SECONDS}s', flush=True)

created = 0
skipped = {}
for row in rows:
    token_id, mode, current_price, current_return, max_pump, snapshots, bsr, strategy, entry_threshold, trades, winrate, avg_pnl, med_pnl, worst_pnl = row

    def skip(reason, note=''):
        skipped[reason] = skipped.get(reason, 0) + 1
        if note:
            log_skip(token_id, mode, reason, note)

    if current_price is None or current_return is None or max_pump is None:
        skip('missing_price')
        continue
    if snapshots < MIN_SNAPSHOTS:
        skip('too_few_snapshots')
        continue

    reversal = max_pump - current_return
    cluster_name, cluster_risk = classify_cluster(max_pump, reversal, bsr, snapshots)
    if cluster_name in ('mega_runner_live', 'strong_continuation_live'):
        skip('dangerous_runner_cluster', f'{cluster_name}; pump={max_pump:.2f}; reversal={reversal:.2f}')
        continue
    if max_pump < MIN_PUMP:
        skip('pump_too_small')
        continue
    if reversal < MIN_REVERSAL and not (ALLOW_NO_REVERSAL_FOR_CRACK and mode == 'CRACK'):
        skip('no_reversal_yet')
        continue

    threshold_pct = (entry_threshold - 1.0) * 100.0
    if max_pump < threshold_pct:
        skip('strategy_threshold_not_met')
        continue

    conf = confidence_score(winrate, avg_pnl, med_pnl, worst_pnl, trades, mode, cluster_risk, reversal)
    if cluster_name in ('pump_reversal_live', 'pump_exhaustion_live'):
        conf = int(clamp(conf + 4, 1, 99))

    adapt_adj, adapt_reason = adaptive_adjustment(mode, cluster_name, cluster_risk)
    if adapt_adj <= -100:
        skip('adaptive_disabled_setup', adapt_reason)
        continue
    conf = int(clamp(conf + adapt_adj, 1, 99))
    if conf < MIN_CONFIDENCE:
        skip('low_confidence')
        continue

    cur.execute("SELECT 1 FROM live_signals WHERE token_id=%s AND UPPER(side)='SHORT' AND created_at >= NOW() - interval '30 minutes' LIMIT 1", (token_id,))
    if cur.fetchone():
        skip('duplicate_recent')
        continue

    entry_low = current_price * 0.98
    entry_high = current_price * 1.02
    tp = current_price * (1.0 - SHORT_TP_PCT / 100.0)
    sl = current_price * (1.0 + SHORT_SL_PCT / 100.0)
    max_lev = 2.0 if mode == 'CRACK' and conf >= 85 else (1.5 if mode in ('CRACK', 'FLASH') and conf >= 75 else 1.0)

    cur.execute('''
    INSERT INTO live_signals(
        token_id, mode, side, signal_type, confidence, confidence_pct,
        current_price, entry_low, entry_high, take_profit_price, stop_loss_price, max_leverage,
        current_return_pct, matched_strategy,
        historical_trades, historical_winrate, historical_avg_pnl, historical_median_pnl, historical_worst_pnl,
        reason, live_snapshots, max_pump_pct, reversal_from_peak_pct, cluster_name, cluster_risk,
        adaptive_adjustment, adaptive_reason, created_at
    ) VALUES (%s,%s,'SHORT','SHORT_SIGNAL',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    RETURNING id
    ''', (
        token_id, mode, confidence_label(conf), conf,
        current_price, entry_low, entry_high, tp, sl, max_lev,
        current_return, strategy,
        trades, winrate, avg_pnl, med_pnl, worst_pnl,
        f'{mode} matched {strategy}; cluster={cluster_name}/{cluster_risk}; adaptive={adapt_reason}',
        snapshots, max_pump, reversal, cluster_name, cluster_risk,
        adapt_adj, adapt_reason, utcnow(),
    ))
    signal_id = cur.fetchone()[0]

    cur.execute('''
    INSERT INTO paper_signal_trades(
      signal_id, token_id, mode, side, status, confidence_pct, entry_price,
      take_profit_price, stop_loss_price, max_leverage, opened_at
    ) VALUES (%s,%s,%s,'SHORT','OPEN',%s,%s,%s,%s,%s,%s)
    ON CONFLICT (signal_id) DO NOTHING
    ''', (signal_id, token_id, mode, conf, current_price, tp, sl, max_lev, utcnow()))
    created += 1

print(f'live signals created: {created}', flush=True)
print(
    'signal skips: '
    f"dangerous_runner_cluster={skipped.get('dangerous_runner_cluster', 0)} "
    f"no_reversal_yet={skipped.get('no_reversal_yet', 0)} "
    f"too_few_snapshots={skipped.get('too_few_snapshots', 0)} "
    f"low_confidence={skipped.get('low_confidence', 0)} "
    f"duplicate_recent={skipped.get('duplicate_recent', 0)}",
    flush=True,
)
