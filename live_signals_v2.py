import os
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
STALE_SECONDS = int(os.getenv('LIVE_SIGNALS_STALE_SECONDS', '60'))
MAX_AGE_SECONDS = float(os.getenv('LIVE_SIGNALS_MAX_AGE_SECONDS', '900'))
MIN_SNAPSHOTS = int(os.getenv('LIVE_SIGNALS_MIN_SNAPSHOTS', '3'))
MIN_TRADES = int(os.getenv('LIVE_SIGNALS_MIN_TRADES', '50'))
MIN_WINRATE = float(os.getenv('LIVE_SIGNALS_MIN_WINRATE', '0.55'))
MIN_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_MIN_EXPECTANCY', '5'))
MIN_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_MIN_CONFIDENCE', '65'))
MIN_MEDIAN_PNL = float(os.getenv('LIVE_SIGNALS_MIN_MEDIAN_PNL', '5'))
MAX_WORST_PNL = float(os.getenv('LIVE_SIGNALS_MAX_WORST_PNL', '-70'))
MAX_STOP_DISTANCE_PCT = float(os.getenv('LIVE_SIGNALS_MAX_STOP_DISTANCE_PCT', '120'))
MIN_REWARD_RISK = float(os.getenv('LIVE_SIGNALS_MIN_REWARD_RISK', '0.35'))
SHORT_MIN_CURRENT_RETURN_PCT = float(os.getenv('LIVE_SIGNALS_SHORT_MIN_CURRENT_RETURN_PCT', '5'))
LONG_MIN_TRADES = int(os.getenv('LIVE_SIGNALS_LONG_MIN_TRADES', '50'))
LONG_MIN_WINRATE = float(os.getenv('LIVE_SIGNALS_LONG_MIN_WINRATE', '0.55'))
LONG_MIN_EXPECTANCY = float(os.getenv('LIVE_SIGNALS_LONG_MIN_EXPECTANCY', '5'))
LONG_MIN_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_LONG_MIN_CONFIDENCE', str(MIN_CONFIDENCE)))
MIN_PUMP = float(os.getenv('LIVE_SIGNALS_MIN_PUMP_FOR_REVERSAL_PCT', '40'))
MIN_REVERSAL = float(os.getenv('LIVE_SIGNALS_MIN_REVERSAL_FROM_PEAK_PCT', '10'))
SHORT_TP_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_TP_PCT', '12'))
SHORT_SL_PCT = float(os.getenv('LIVE_SIGNAL_SHORT_SL_PCT', '18'))
LONG_TP_PCT = float(os.getenv('LIVE_SIGNAL_LONG_TP_PCT', '12'))
LONG_SL_PCT = float(os.getenv('LIVE_SIGNAL_LONG_SL_PCT', '18'))
CAP_EXECUTION_STOP = os.getenv('LIVE_SIGNALS_CAP_EXECUTION_STOP', 'true').lower() in ('1', 'true', 'yes', 'on')
MAX_STRATEGIES_PER_TOKEN_SIDE = int(os.getenv('LIVE_SIGNALS_MAX_STRATEGIES_PER_TOKEN_SIDE', '5'))
DEBUG_LOG_LIMIT_PER_STATUS = int(os.getenv('LIVE_SIGNALS_DEBUG_LOG_LIMIT_PER_STATUS', '5'))
SKIP_SCHEMA_ENSURE = os.getenv('LIVE_SIGNALS_SKIP_SCHEMA_ENSURE', 'false').lower() in ('1', 'true', 'yes', 'on')
OPEN_PAPER_IN_GENERATOR = os.getenv('LIVE_SIGNALS_OPEN_PAPER_IN_GENERATOR', 'false').lower() in ('1', 'true', 'yes', 'on')
ADAPTIVE_ENABLED = os.getenv('LIVE_SIGNALS_ADAPTIVE_LIVE_ENABLED', 'true').lower() in ('1', 'true', 'yes', 'on')
ADAPTIVE_LOOKBACK_DAYS = int(os.getenv('LIVE_SIGNALS_ADAPTIVE_LOOKBACK_DAYS', '30'))
ADAPTIVE_MIN_CLOSED_TRADES = int(os.getenv('LIVE_SIGNALS_ADAPTIVE_MIN_CLOSED_TRADES', '5'))
ADAPTIVE_DISABLE_WINRATE = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_DISABLE_WINRATE', '0.55'))
ADAPTIVE_DISABLE_AVG_PNL = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_DISABLE_AVG_PNL', '1'))
ADAPTIVE_BOOST_WINRATE = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_BOOST_WINRATE', '0.60'))
ADAPTIVE_BOOST_AVG_PNL = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_BOOST_AVG_PNL', '5'))
ADAPTIVE_BOOST_CONFIDENCE = int(os.getenv('LIVE_SIGNALS_ADAPTIVE_BOOST_CONFIDENCE', '5'))
ADAPTIVE_ALLOW_UNLEARNED = os.getenv('LIVE_SIGNALS_ADAPTIVE_ALLOW_UNLEARNED', 'false').lower() in ('1', 'true', 'yes', 'on')
BUY_BUCKET_MIN_CLOSED = int(os.getenv('LIVE_SIGNALS_BUY_BUCKET_MIN_CLOSED', '8'))
BUY_BUCKET_SIZE = float(os.getenv('LIVE_SIGNALS_BUY_BUCKET_SIZE', '1'))
SHORT_MIN_SELLS = int(os.getenv('LIVE_SIGNALS_SHORT_MIN_SELLS', '1'))
SHORT_MAX_UNLEARNED_BUY_SELL_RATIO = float(os.getenv('LIVE_SIGNALS_SHORT_MAX_UNLEARNED_BUY_SELL_RATIO', '4'))
MAX_NEW_SIGNALS_PER_CYCLE = int(os.getenv('LIVE_SIGNALS_MAX_NEW_SIGNALS_PER_CYCLE', '2'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()
debug_examples = {}


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def execute_ddl_batch(statements):
    if statements:
        cur.execute(';\n'.join(statements) + ';')


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
    execute_ddl_batch([
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS live_snapshots INT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS max_pump_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reversal_from_peak_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_name TEXT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_risk TEXT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS adaptive_adjustment INT DEFAULT 0',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS adaptive_reason TEXT',
        "ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS signal_status TEXT DEFAULT 'OPEN'",
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS liquidation_price DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reward_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS risk_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reward_risk DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS buy_sell_ratio DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS buys INT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS sells INT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS traders INT',
    ])

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
    execute_ddl_batch([
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS liquidation_price DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS reward_pct DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS risk_pct DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS reward_risk DOUBLE PRECISION',
    ])
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
    execute_ddl_batch([
        'CREATE INDEX IF NOT EXISTS idx_live_token_features_last_seen ON live_token_features(last_seen)',
        'CREATE INDEX IF NOT EXISTS idx_live_token_features_mode_last_seen ON live_token_features(UPPER(mode), last_seen)',
        'CREATE INDEX IF NOT EXISTS idx_strategy_sweep_mode_side_quality ON strategy_sweep(UPPER(mode), UPPER(side), trades, winrate, avg_pnl)',
        'CREATE INDEX IF NOT EXISTS idx_live_signals_token_side_created ON live_signals(token_id, UPPER(side), created_at)',
        'CREATE INDEX IF NOT EXISTS idx_signal_debug_log_ts_status ON signal_debug_log(ts, status)',
    ])


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


def liquidation_price_for(side, current_price, leverage):
    if not leverage or leverage <= 1:
        return None
    if side == 'SHORT':
        return current_price * (1.0 + 1.0 / leverage)
    return current_price * (1.0 - 1.0 / leverage)


def prices_for_signal(side, current_price, current_return_pct=None, strategy_tp=None, strategy_sl=None):
    entry_low = current_price * 0.98
    entry_high = current_price * 1.02
    if side == 'SHORT':
        tp = None
        sl = None
        fallback_tp = current_price * (1.0 - SHORT_TP_PCT / 100.0)
        fallback_sl = current_price * (1.0 + SHORT_SL_PCT / 100.0)
        if current_return_pct is not None and current_return_pct > -99:
            initial_price = current_price / (1.0 + current_return_pct / 100.0)
            if strategy_tp and 0 < strategy_tp < 1:
                tp = initial_price * strategy_tp
            if strategy_sl and strategy_sl > 1:
                sl = initial_price * strategy_sl
        if tp is None or tp >= current_price:
            tp = fallback_tp
        elif CAP_EXECUTION_STOP:
            tp = max(tp, fallback_tp)
        if sl is None or sl <= current_price:
            sl = fallback_sl
        elif CAP_EXECUTION_STOP:
            sl = min(sl, fallback_sl)
    else:
        tp_mult = strategy_tp if strategy_tp and strategy_tp > 1 else 1.0 + LONG_TP_PCT / 100.0
        sl_mult = strategy_sl if strategy_sl and 0 < strategy_sl < 1 else 1.0 - LONG_SL_PCT / 100.0
        tp = current_price * tp_mult
        sl = current_price * sl_mult
        if CAP_EXECUTION_STOP:
            tp = min(tp, current_price * (1.0 + LONG_TP_PCT / 100.0))
            sl = max(sl, current_price * (1.0 - LONG_SL_PCT / 100.0))
    return entry_low, entry_high, tp, sl


def risk_metrics(side, current_price, take_profit_price, stop_loss_price):
    if not current_price or current_price <= 0 or not take_profit_price or not stop_loss_price:
        return None, None, None
    if side == 'SHORT':
        reward_pct = max(0, (current_price / take_profit_price - 1.0) * 100.0)
        risk_pct = max(0, (stop_loss_price / current_price - 1.0) * 100.0)
    else:
        reward_pct = max(0, (take_profit_price / current_price - 1.0) * 100.0)
        risk_pct = max(0, (current_price / stop_loss_price - 1.0) * 100.0)
    reward_risk = reward_pct / risk_pct if risk_pct > 0 else None
    return reward_pct, risk_pct, reward_risk


def fmt_pct(value):
    return 'n/a' if value is None else f'{float(value):.2f}%'


def log_skip(token_id, mode, status, note):
    if DEBUG_LOG_LIMIT_PER_STATUS <= 0:
        return
    bucket = debug_examples.setdefault(status, [])
    if len(bucket) >= DEBUG_LOG_LIMIT_PER_STATUS:
        return
    bucket.append((utcnow(), str(token_id), mode, status, note[:1000]))


def flush_debug_examples():
    rows = [row for bucket in debug_examples.values() for row in bucket]
    if not rows:
        return
    cur.executemany(
        'INSERT INTO signal_debug_log(ts, token_id, mode, status, note) VALUES (%s,%s,%s,%s,%s)',
        rows,
    )


def adaptive_performance(mode, side, strategy, exact_strategy=True):
    strategy_sql = 'AND ls.matched_strategy = %s' if exact_strategy else ''
    params = [ADAPTIVE_LOOKBACK_DAYS, mode, side]
    if exact_strategy:
        params.append(strategy)
    cur.execute(f'''
    SELECT
        COUNT(*) AS closed,
        COUNT(*) FILTER (WHERE pst.pnl_pct > 0) AS wins,
        AVG(pst.pnl_pct) AS avg_pnl
    FROM paper_signal_trades pst
    JOIN live_signals ls ON ls.id = pst.signal_id
    WHERE pst.status = 'CLOSED'
      AND pst.closed_at >= NOW() - (%s || ' days')::interval
      AND UPPER(ls.mode) = %s
      AND UPPER(ls.side) = %s
      {strategy_sql}
    ''', params)
    closed, wins, avg_pnl = cur.fetchone()
    closed = int(closed or 0)
    wins = int(wins or 0)
    winrate = wins / closed if closed else None
    return closed, winrate, avg_pnl


def performance_blocks(closed, winrate, avg_pnl):
    avg = float(avg_pnl or 0)
    return winrate is not None and (winrate < ADAPTIVE_DISABLE_WINRATE or avg < ADAPTIVE_DISABLE_AVG_PNL)


def adaptive_gate(mode, side, strategy):
    if not ADAPTIVE_ENABLED:
        return False, 0, 'smart=off'

    closed, winrate, avg_pnl = adaptive_performance(mode, side, strategy, exact_strategy=True)
    if closed >= ADAPTIVE_MIN_CLOSED_TRADES:
        avg = float(avg_pnl or 0)
        if performance_blocks(closed, winrate, avg_pnl):
            return True, 0, f'smart blocked strategy n={closed} win={winrate*100:.1f}% avg={avg:.2f}%'
        if winrate is not None and winrate >= ADAPTIVE_BOOST_WINRATE and avg >= ADAPTIVE_BOOST_AVG_PNL:
            return False, ADAPTIVE_BOOST_CONFIDENCE, f'smart boost strategy n={closed} win={winrate*100:.1f}% avg={avg:.2f}%'
        return False, 0, f'smart ok strategy n={closed} win={(winrate or 0)*100:.1f}% avg={avg:.2f}%'

    broad_closed, broad_winrate, broad_avg = adaptive_performance(mode, side, strategy, exact_strategy=False)
    if broad_closed >= ADAPTIVE_MIN_CLOSED_TRADES:
        avg = float(broad_avg or 0)
        if performance_blocks(broad_closed, broad_winrate, broad_avg):
            return True, 0, f'smart blocked mode n={broad_closed} win={broad_winrate*100:.1f}% avg={avg:.2f}%'
        return False, 0, f'smart explore strategy n={closed}/{ADAPTIVE_MIN_CLOSED_TRADES}; mode ok n={broad_closed} win={(broad_winrate or 0)*100:.1f}% avg={avg:.2f}%'

    if not ADAPTIVE_ALLOW_UNLEARNED:
        return True, 0, f'smart blocked unlearned n={closed}/{ADAPTIVE_MIN_CLOSED_TRADES}'

    return False, 0, f'smart learning n={closed}/{ADAPTIVE_MIN_CLOSED_TRADES}'


def buy_pressure_bucket(value):
    if value is None or BUY_BUCKET_SIZE <= 0:
        return None
    value = max(0.0, float(value))
    return int(value / BUY_BUCKET_SIZE) * BUY_BUCKET_SIZE


def buy_pressure_gate(side, buy_sell_ratio, buys, sells, traders):
    side = str(side or '').upper()
    if side != 'SHORT':
        return False, 'buy learning only for shorts'
    sells = int(sells or 0)
    buys = int(buys or 0)
    traders = int(traders or 0)
    if sells < SHORT_MIN_SELLS:
        return True, f'buy pressure blocked sells={sells} min={SHORT_MIN_SELLS}'

    bucket = buy_pressure_bucket(buy_sell_ratio)
    if bucket is None:
        return False, 'buy pressure n/a'

    low = bucket
    high = bucket + BUY_BUCKET_SIZE
    cur.execute('''
    SELECT
        COUNT(*) AS closed,
        COUNT(*) FILTER (WHERE pst.pnl_pct > 0) AS wins,
        AVG(pst.pnl_pct) AS avg_pnl
    FROM paper_signal_trades pst
    JOIN live_signals ls ON ls.id = pst.signal_id
    WHERE pst.status = 'CLOSED'
      AND pst.closed_at >= NOW() - (%s || ' days')::interval
      AND UPPER(ls.side) = %s
      AND ls.buy_sell_ratio >= %s
      AND ls.buy_sell_ratio < %s
    ''', (ADAPTIVE_LOOKBACK_DAYS, side, low, high))
    closed, wins, avg_pnl = cur.fetchone()
    closed = int(closed or 0)
    wins = int(wins or 0)
    winrate = wins / closed if closed else None
    if closed >= BUY_BUCKET_MIN_CLOSED:
        avg = float(avg_pnl or 0)
        if performance_blocks(closed, winrate, avg_pnl):
            return True, f'buy bucket blocked {low:.1f}-{high:.1f} n={closed} win={winrate*100:.1f}% avg={avg:.2f}%'
        return False, f'buy bucket ok {low:.1f}-{high:.1f} n={closed} win={(winrate or 0)*100:.1f}% avg={avg:.2f}%'

    if buy_sell_ratio is not None and float(buy_sell_ratio) > SHORT_MAX_UNLEARNED_BUY_SELL_RATIO:
        return True, f'buy pressure blocked unlearned ratio={float(buy_sell_ratio):.2f} n={closed}/{BUY_BUCKET_MIN_CLOSED} buys={buys} sells={sells} traders={traders}'
    return False, f'buy pressure learning ratio={float(buy_sell_ratio):.2f} n={closed}/{BUY_BUCKET_MIN_CLOSED} buys={buys} sells={sells} traders={traders}'


if not SKIP_SCHEMA_ENSURE:
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
  AND median_pnl >= %s
  AND worst_pnl >= %s
  AND UPPER(side) = 'SHORT'
GROUP BY UPPER(side)
UNION ALL
SELECT UPPER(side), COUNT(*)
FROM strategy_sweep
WHERE trades >= %s
  AND winrate >= %s
  AND avg_pnl >= %s
  AND median_pnl >= %s
  AND worst_pnl >= %s
  AND UPPER(side) = 'LONG'
GROUP BY UPPER(side)
''', (
    MIN_TRADES, MIN_WINRATE, MIN_EXPECTANCY, MIN_MEDIAN_PNL, MAX_WORST_PNL,
    LONG_MIN_TRADES, LONG_MIN_WINRATE, LONG_MIN_EXPECTANCY, MIN_MEDIAN_PNL, MAX_WORST_PNL,
))
strategy_counts = {side: count for side, count in cur.fetchall()}
strategy_count = sum(strategy_counts.values())

cur.execute('''
WITH candidate_base AS (
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
        lf.buys,
        lf.sells,
        lf.traders,
        UPPER(ss.side) AS side,
        ss.strategy,
        ss.entry_threshold,
        ss.take_profit,
        ss.stop_loss,
        ss.trades,
        ss.winrate,
        ss.avg_pnl,
        ss.median_pnl,
        ss.worst_pnl,
        ROW_NUMBER() OVER (
            PARTITION BY lf.token_id, UPPER(ss.side)
            ORDER BY ss.avg_pnl DESC, ss.winrate DESC, ss.trades DESC, ss.median_pnl DESC
        ) AS strategy_rank
    FROM live_token_features lf
    JOIN strategy_sweep ss ON UPPER(ss.mode) = UPPER(lf.mode)
    WHERE lf.last_seen >= NOW() - (%s || ' seconds')::interval
      AND lf.age_seconds <= %s
      AND lf.current_price IS NOT NULL
      AND lf.current_return_pct IS NOT NULL
      AND lf.max_pump_pct IS NOT NULL
      AND ss.median_pnl >= %s
      AND ss.worst_pnl >= %s
      AND (
          (
              UPPER(ss.side) = 'SHORT'
              AND ss.trades >= %s
              AND ss.winrate >= %s
              AND ss.avg_pnl >= %s
              AND lf.current_return_pct >= %s
              AND lf.max_pump_pct >= %s
              AND (lf.max_pump_pct - lf.current_return_pct) >= %s
              AND lf.max_pump_pct >= ((ss.entry_threshold - 1.0) * 100.0)
          )
          OR (
              UPPER(ss.side) = 'LONG'
              AND ss.trades >= %s
              AND ss.winrate >= %s
              AND ss.avg_pnl >= %s
              AND lf.max_drawdown_pct IS NOT NULL
              AND lf.current_return_pct <= ((ss.entry_threshold - 1.0) * 100.0)
          )
      )
)
SELECT
    token_id, mode, current_price, current_return_pct, max_pump_pct, max_drawdown_pct,
    age_seconds, snapshots, buy_sell_ratio, buys, sells, traders, side, strategy, entry_threshold, take_profit,
    stop_loss, trades, winrate, avg_pnl, median_pnl, worst_pnl
FROM candidate_base
WHERE strategy_rank <= %s
ORDER BY avg_pnl DESC, winrate DESC, trades DESC, token_id, side, strategy_rank
''', (
    STALE_SECONDS, MAX_AGE_SECONDS, MIN_MEDIAN_PNL, MAX_WORST_PNL,
    MIN_TRADES, MIN_WINRATE, MIN_EXPECTANCY, SHORT_MIN_CURRENT_RETURN_PCT, MIN_PUMP, MIN_REVERSAL,
    LONG_MIN_TRADES, LONG_MIN_WINRATE, LONG_MIN_EXPECTANCY,
    MAX_STRATEGIES_PER_TOKEN_SIDE,
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
        bsr, buys, sells, traders, side, strategy, entry_threshold, take_profit, stop_loss,
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
    if MAX_NEW_SIGNALS_PER_CYCLE > 0 and created >= MAX_NEW_SIGNALS_PER_CYCLE:
        break

    threshold_pct = (entry_threshold - 1.0) * 100.0
    if side == 'SHORT':
        setup_move = max_pump - current_return
        cluster_name, cluster_risk = classify_short_cluster(max_pump, current_return, bsr, snapshots)

        if current_return < SHORT_MIN_CURRENT_RETURN_PCT:
            skip('current_return_too_low', f'current={current_return:.2f}% min={SHORT_MIN_CURRENT_RETURN_PCT:.2f}%')
            continue
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
        buy_blocked, buy_reason = buy_pressure_gate(side, bsr, buys, sells, traders)
        if buy_blocked:
            skip('buy_pressure_blocked', buy_reason)
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

    entry_low, entry_high, tp, sl = prices_for_signal(side, current_price, current_return, take_profit, stop_loss)
    reward_pct, risk_pct, reward_risk = risk_metrics(side, current_price, tp, sl)
    if avg_pnl is None or avg_pnl < (LONG_MIN_EXPECTANCY if side == 'LONG' else MIN_EXPECTANCY):
        skip('low_expectancy', f'avg={fmt_pct(avg_pnl)}')
        continue
    if med_pnl is None or med_pnl < MIN_MEDIAN_PNL:
        skip('low_median_pnl', f'median={fmt_pct(med_pnl)}')
        continue
    if worst_pnl is None or worst_pnl < MAX_WORST_PNL:
        skip('worst_pnl_too_bad', f'worst={fmt_pct(worst_pnl)}')
        continue
    if risk_pct is None or risk_pct > MAX_STOP_DISTANCE_PCT:
        skip('stop_too_far', f'risk={fmt_pct(risk_pct)} max={MAX_STOP_DISTANCE_PCT:.2f}%')
        continue
    if reward_risk is None or reward_risk < MIN_REWARD_RISK:
        skip('bad_reward_risk', f'rr={reward_risk if reward_risk is not None else "n/a"} min={MIN_REWARD_RISK:.2f}')
        continue

    conf = confidence(winrate, avg_pnl, med_pnl, worst_pnl, trades, mode, side, cluster_risk, setup_move)
    adaptive_blocked, adaptive_adjustment, adaptive_reason = adaptive_gate(mode, side, strategy)
    if adaptive_blocked:
        skip('smart_blocked', adaptive_reason)
        continue
    if adaptive_adjustment:
        conf = int(round(clamp(conf + adaptive_adjustment, 1, 99)))

    min_confidence = LONG_MIN_CONFIDENCE if side == 'LONG' else MIN_CONFIDENCE
    if conf < min_confidence:
        skip('low_confidence')
        continue

    cur.execute('''
    SELECT 1 FROM live_signals
    WHERE token_id = %s AND UPPER(side) = %s AND created_at >= NOW() - interval '30 minutes'
      AND COALESCE(signal_status, 'OPEN') <> 'CANCELLED'
      AND COALESCE(reason, '') NOT LIKE '%%CANCELLED_QUALITY_GATE%%'
    LIMIT 1
    ''', (token_id, side))
    if cur.fetchone():
        skip('duplicate_recent')
        continue

    lev = leverage_for(conf, mode, side)
    liquidation_price = liquidation_price_for(side, current_price, lev)
    if liquidation_price is not None:
        if side == 'SHORT' and sl >= liquidation_price:
            skip('stop_after_liquidation', f'sl={sl:.8f} liquidation={liquidation_price:.8f}')
            continue
        if side == 'LONG' and sl <= liquidation_price:
            skip('stop_after_liquidation', f'sl={sl:.8f} liquidation={liquidation_price:.8f}')
            continue
    label = 'HIGH' if conf >= 80 else 'MEDIUM' if conf >= 65 else 'LOW'
    bounce_note = bounce_from_low if side == 'LONG' else setup_move
    reason = (
        f'{mode} {strategy} cluster={cluster_name}/{cluster_risk} '
        f'current={current_return:.2f}% max_pump={max_pump:.2f}% max_dd={fmt_pct(max_dd)} '
        f'setup_move={setup_move:.2f}% bounce_from_low={bounce_note:.2f}% '
        f'reward={reward_pct:.2f}% risk={risk_pct:.2f}% rr={reward_risk:.2f} '
        f'{adaptive_reason} '
        f'snapshots={snapshots} historical_winrate={winrate*100:.1f}% avg={avg_pnl:.2f}% trades={trades}'
    )

    cur.execute('''
    INSERT INTO live_signals(
        token_id, mode, side, signal_type, confidence, confidence_pct,
        current_price, entry_low, entry_high, take_profit_price, stop_loss_price, max_leverage,
        current_return_pct, matched_strategy,
        historical_trades, historical_winrate, historical_avg_pnl, historical_median_pnl,
        historical_worst_pnl, reason, created_at, signal_status,
        live_snapshots, max_pump_pct, reversal_from_peak_pct, cluster_name, cluster_risk,
        adaptive_adjustment, adaptive_reason, liquidation_price, reward_pct, risk_pct, reward_risk,
        buy_sell_ratio, buys, sells, traders
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    RETURNING id
    ''', (
        token_id, mode, side, f'{side}_SIGNAL', label, conf,
        current_price, entry_low, entry_high, tp, sl, lev,
        current_return, strategy,
        trades, winrate, avg_pnl, med_pnl, worst_pnl, reason, utcnow(),
        snapshots, max_pump, setup_move, cluster_name, cluster_risk,
        adaptive_adjustment, adaptive_reason,
        liquidation_price, reward_pct, risk_pct, reward_risk,
        bsr, buys, sells, traders,
    ))
    signal_id = cur.fetchone()[0]

    if OPEN_PAPER_IN_GENERATOR:
        cur.execute('''
        INSERT INTO paper_signal_trades(
            signal_id, token_id, mode, side, status, confidence_pct,
            entry_price, take_profit_price, stop_loss_price, max_leverage, opened_at,
            liquidation_price, reward_pct, risk_pct, reward_risk
        ) VALUES (%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(signal_id) DO NOTHING
        ''', (
            signal_id, token_id, mode, side, conf, current_price, tp, sl, lev, utcnow(),
            liquidation_price, reward_pct, risk_pct, reward_risk,
        ))
    created += 1

flush_debug_examples()
print(f'live signals created: {created}', flush=True)
if skipped:
    print('signal skips:', ', '.join(f'{k}={v}' for k, v in sorted(skipped.items())), flush=True)
else:
    print('signal skips: none - no joined candidates or all candidates became signals', flush=True)

cur.execute('''
SELECT token_id, mode, side, confidence_pct, current_price, take_profit_price, stop_loss_price,
       matched_strategy, cluster_name, reversal_from_peak_pct
FROM live_signals
WHERE COALESCE(signal_status, 'OPEN') <> 'CANCELLED'
  AND COALESCE(reason, '') NOT LIKE '%CANCELLED_QUALITY_GATE%'
ORDER BY confidence_pct DESC NULLS LAST, historical_avg_pnl DESC NULLS LAST, id DESC
LIMIT 20
''')
for row in cur.fetchall():
    print(
        f'{row[0]} {row[1]} {row[2]} conf={row[3]}% price={row[4]:.6f} tp={row[5]:.6f} sl={row[6]:.6f} strategy={row[7]} cluster={row[8]} setup={row[9]:.2f}%',
        flush=True,
    )
