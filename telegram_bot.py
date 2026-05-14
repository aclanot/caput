import csv
import io
import os
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone

import psycopg
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')
BACKUP_MAX_ROWS_PER_TABLE = int(os.getenv('BACKUP_MAX_ROWS_PER_TABLE', '0'))
BACKUP_INCLUDE_SNAPSHOTS = os.getenv('BACKUP_INCLUDE_SNAPSHOTS', 'true').lower() in ('1', 'true', 'yes', 'on')
PG_DUMP_TIMEOUT_SECONDS = int(os.getenv('PG_DUMP_TIMEOUT_SECONDS', '600'))
TELEGRAM_MAX_UPLOAD_MB = float(os.getenv('TELEGRAM_MAX_UPLOAD_MB', '45'))
PAPER_START_BALANCE_USDT = float(os.getenv('PAPER_START_BALANCE_USDT', '1000'))
PAPER_TRADE_SIZE_USDT = float(os.getenv('PAPER_TRADE_SIZE_USDT', '100'))
PAPER_AUTO_OPEN = os.getenv('PAPER_AUTO_OPEN', 'true').lower() in ('1', 'true', 'yes', 'on')
PAPER_MAX_OPEN_TRADES = int(os.getenv('PAPER_MAX_OPEN_TRADES', '10'))
PAPER_MAX_POSITION_PCT = float(os.getenv('PAPER_MAX_POSITION_PCT', '10'))
PG_DUMP_ESSENTIAL_TABLES = [
    'finished_tokens',
    'trajectory_features',
    'paper_trades',
    'strategy_sweep',
    'trajectory_clusters',
]
PG_DUMP_CORE_TABLES = [
    'finished_tokens',
    'trajectory_features',
    'paper_trades',
    'strategy_sweep',
    'trajectory_clusters',
    'official_api_token_state',
    'live_token_features',
    'live_signals',
]

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True

BACKUP_TABLES = [
    'finished_tokens',
    'trajectory_features',
    'paper_trades',
    'strategy_sweep',
    'trajectory_clusters',
    'official_api_token_state',
    'token_snapshots',
    'live_token_features',
    'live_signals',
]


def table_exists(cur, table_name):
    cur.execute('SELECT to_regclass(%s)', (f'public.{table_name}',))
    return cur.fetchone()[0] is not None


def column_exists(cur, table_name, column_name):
    cur.execute('''
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
    ''', (table_name, column_name))
    return cur.fetchone() is not None


def ensure_live_signal_columns(cur):
    if not table_exists(cur, 'live_signals'):
        return
    cur.execute("ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS signal_status TEXT DEFAULT 'OPEN'")


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_paper_schema(cur):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS paper_account (
        account_key TEXT PRIMARY KEY,
        balance_usdt DOUBLE PRECISION,
        updated_at TIMESTAMP
    )
    ''')
    cur.execute('''
    INSERT INTO paper_account(account_key, balance_usdt, updated_at)
    VALUES ('default', %s, %s)
    ON CONFLICT(account_key) DO NOTHING
    ''', (PAPER_START_BALANCE_USDT, utcnow()))

    cur.execute('''
    CREATE TABLE IF NOT EXISTS paper_settings (
        setting_key TEXT PRIMARY KEY,
        setting_value TEXT,
        updated_at TIMESTAMP
    )
    ''')
    cur.executemany('''
    INSERT INTO paper_settings(setting_key, setting_value, updated_at)
    VALUES (%s, %s, %s)
    ON CONFLICT(setting_key) DO NOTHING
    ''', [
        ('auto_open', 'true' if PAPER_AUTO_OPEN else 'false', utcnow()),
        ('trade_size_usdt', str(PAPER_TRADE_SIZE_USDT), utcnow()),
        ('max_open_trades', str(PAPER_MAX_OPEN_TRADES), utcnow()),
        ('max_position_pct', str(PAPER_MAX_POSITION_PCT), utcnow()),
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
    for ddl in [
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS result_sent_to_telegram BOOLEAN DEFAULT FALSE',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS virtual_position_usdt DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS virtual_balance_at_open DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS virtual_pnl_usdt DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS virtual_balance_after_close DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS liquidation_price DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS reward_pct DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS risk_pct DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS reward_risk DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS capital_reserved BOOLEAN DEFAULT FALSE',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS last_mark_price DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS last_pnl_pct DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS last_pnl_usdt DOUBLE PRECISION',
        'ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS last_marked_at TIMESTAMP',
    ]:
        cur.execute(ddl)
    reserve_existing_open_positions(cur)


def paper_setting(cur, key, default):
    cur.execute('SELECT setting_value FROM paper_settings WHERE setting_key = %s', (key,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else default


def set_paper_setting(cur, key, value):
    cur.execute('''
    INSERT INTO paper_settings(setting_key, setting_value, updated_at)
    VALUES (%s, %s, %s)
    ON CONFLICT(setting_key) DO UPDATE SET
        setting_value = EXCLUDED.setting_value,
        updated_at = EXCLUDED.updated_at
    ''', (key, str(value), utcnow()))


def paper_pnl_pct(side, entry_price, current_price, leverage, close_reason=None):
    if not entry_price or entry_price <= 0 or current_price is None:
        return None
    side = str(side or '').upper()
    leverage = leverage or 1.0
    if close_reason == 'LIQUIDATION':
        return -100.0
    if side == 'SHORT':
        raw_pct = (1.0 - current_price / entry_price) * 100.0
    else:
        raw_pct = (current_price / entry_price - 1.0) * 100.0
    return max(-100.0, raw_pct * leverage)


def account_balance(cur):
    cur.execute("SELECT balance_usdt FROM paper_account WHERE account_key = 'default'")
    row = cur.fetchone()
    if row:
        return float(row[0] or 0)
    cur.execute('''
    INSERT INTO paper_account(account_key, balance_usdt, updated_at)
    VALUES ('default', %s, %s)
    RETURNING balance_usdt
    ''', (PAPER_START_BALANCE_USDT, utcnow()))
    return float(cur.fetchone()[0])


def update_account_balance(cur, delta_usdt):
    balance = account_balance(cur)
    new_balance = balance + float(delta_usdt)
    cur.execute('''
    UPDATE paper_account
    SET balance_usdt = %s, updated_at = %s
    WHERE account_key = 'default'
    ''', (new_balance, utcnow()))
    return new_balance


def reserve_existing_open_positions(cur):
    if not table_exists(cur, 'paper_signal_trades') or not table_exists(cur, 'paper_account'):
        return 0
    cur.execute('''
    SELECT id, COALESCE(virtual_position_usdt, %s) AS position_usdt
    FROM paper_signal_trades
    WHERE status = 'OPEN'
      AND COALESCE(capital_reserved, false) = false
    ORDER BY id ASC
    ''', (PAPER_TRADE_SIZE_USDT,))
    rows = cur.fetchall()
    reserved = 0
    for trade_id, position_usdt in rows:
        position_usdt = float(position_usdt or 0)
        if position_usdt <= 0:
            continue
        balance = account_balance(cur)
        if balance < position_usdt:
            break
        balance_after = update_account_balance(cur, -position_usdt)
        cur.execute('''
        UPDATE paper_signal_trades
        SET capital_reserved = true,
            virtual_position_usdt = %s,
            virtual_balance_at_open = %s
        WHERE id = %s
        ''', (position_usdt, balance_after, trade_id))
        reserved += 1
    return reserved


def paper_account_snapshot(cur):
    ensure_paper_schema(cur)
    balance = account_balance(cur)
    cur.execute('''
    SELECT
        pst.id, UPPER(pst.side), pst.entry_price,
        COALESCE(os.current_price, ltf.current_price, pst.last_mark_price) AS current_price,
        pst.max_leverage, pst.virtual_position_usdt,
        pst.liquidation_price, COALESCE(pst.capital_reserved, false),
        pst.last_pnl_usdt
    FROM paper_signal_trades pst
    LEFT JOIN live_token_features ltf ON ltf.token_id = pst.token_id
    LEFT JOIN official_api_token_state os ON os.token_id = pst.token_id
    WHERE pst.status = 'OPEN'
    ''')
    open_rows = cur.fetchall()
    reserved = 0.0
    unreserved = 0.0
    unrealized = 0.0
    priced = 0
    for _, side, entry_price, current_price, leverage, position_usdt, liquidation_price, capital_reserved, last_pnl_usdt in open_rows:
        position_usdt = float(position_usdt or 0)
        if capital_reserved:
            reserved += position_usdt
        else:
            unreserved += position_usdt
        close_reason = None
        if liquidation_price is not None and current_price is not None:
            if side == 'SHORT' and current_price >= liquidation_price:
                close_reason = 'LIQUIDATION'
            elif side == 'LONG' and current_price <= liquidation_price:
                close_reason = 'LIQUIDATION'
        pnl_pct = paper_pnl_pct(side, entry_price, current_price, leverage, close_reason)
        if pnl_pct is not None:
            unrealized += position_usdt * pnl_pct / 100.0
            priced += 1
        elif last_pnl_usdt is not None:
            unrealized += float(last_pnl_usdt or 0)
            priced += 1
    equity = balance + reserved + unreserved + unrealized
    return {
        'free': balance,
        'reserved': reserved,
        'unreserved': unreserved,
        'unrealized': unrealized,
        'equity': equity,
        'open_count': len(open_rows),
        'priced_open_count': priced,
    }


def paper_bool(cur, key, default):
    fallback = 'true' if default else 'false'
    return str(paper_setting(cur, key, fallback)).lower() in ('1', 'true', 'yes', 'on')


def parse_float_arg(context):
    for arg in context.args:
        try:
            return float(arg.replace('$', '').replace(',', ''))
        except ValueError:
            continue
    return None


def existing_tables(table_names):
    cur = conn.cursor()
    return [name for name in table_names if table_exists(cur, name)]


def safe_identifier(name):
    if not name.replace('_', '').isalnum():
        raise ValueError(f'unsafe table name: {name}')
    return name


def utc_stamp():
    return datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')


def write_table_csv_to_zip(cur, zip_file, table_name):
    table_name = safe_identifier(table_name)
    if not table_exists(cur, table_name):
        return 0

    if table_name == 'token_snapshots' and not BACKUP_INCLUDE_SNAPSHOTS:
        return 0

    cur.execute('''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    ''', (table_name,))
    columns = [row[0] for row in cur.fetchall()]
    if not columns:
        return 0

    sql = f'SELECT * FROM {table_name}'
    if BACKUP_MAX_ROWS_PER_TABLE > 0:
        order_col = 'id' if 'id' in columns else columns[0]
        sql += f' ORDER BY {order_col} DESC LIMIT %s'
        cur.execute(sql, (BACKUP_MAX_ROWS_PER_TABLE,))
    else:
        cur.execute(sql)

    rows = cur.fetchall()
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(columns)
    writer.writerows(rows)
    zip_file.writestr(f'{table_name}.csv', csv_buffer.getvalue())
    return len(rows)


def latest_collector_log(cur):
    if not table_exists(cur, 'official_api_collector_log'):
        return None
    has_status = column_exists(cur, 'official_api_collector_log', 'status')
    has_note = column_exists(cur, 'official_api_collector_log', 'note')
    has_event_type = column_exists(cur, 'official_api_collector_log', 'event_type')
    has_error_message = column_exists(cur, 'official_api_collector_log', 'error_message')

    status_expr = 'status' if has_status else 'event_type' if has_event_type else "'unknown'"
    note_expr = 'note' if has_note else 'error_message' if has_error_message else "''"
    cur.execute(f'''
        SELECT ts, {status_expr} AS status, {note_expr} AS note
        FROM official_api_collector_log
        ORDER BY id DESC
        LIMIT 1
    ''')
    return cur.fetchone()


def arg_limit(context, default=10, maximum=50):
    for arg in context.args:
        try:
            value = int(arg)
        except ValueError:
            continue
        return max(1, min(maximum, value))
    return default


def arg_side(context):
    for arg in context.args:
        value = arg.upper()
        if value in ('LONG', 'SHORT'):
            return value
    return None


def fmt_pct(value):
    if value is None:
        return 'n/a'
    return f'{float(value):.2f}%'


def fmt_price(value):
    if value is None:
        return 'n/a'
    value = float(value)
    if abs(value) >= 100:
        return f'{value:.2f}'
    if abs(value) >= 1:
        return f'{value:.4f}'
    return f'{value:.8f}'


def fmt_money(value):
    if value is None:
        return 'n/a'
    return f'${float(value):.2f}'


def token_label(name, symbol, token_id):
    if symbol and name:
        return f'{symbol} ({name})'
    if symbol:
        return str(symbol)
    if name:
        return str(name)
    return f'Token {token_id}'


def token_url(token_id):
    return f'https://catapult.trade/ru/turbo/tokens/{token_id}'


async def reply_text(update, text):
    for start in range(0, len(text), 3900):
        await update.message.reply_text(text[start:start + 3900])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Caput bot\n\n'
        '/stats - balance, winrate, PnL\n'
        '/paper - open and latest paper trades\n'
        '/calls - recent calls\n'
        '/balance add 100 | remove 50 | set 1000\n'
        '/auto on|off - auto paper opens\n'
        '/smart - adaptive learning status\n'
        '/health - pipeline status'
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM finished_tokens')
    finished = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM token_snapshots')
    snapshots = cur.fetchone()[0]
    cur.execute('SELECT MAX(ts) FROM token_snapshots')
    last_snapshot = cur.fetchone()[0]

    features = 0
    if table_exists(cur, 'trajectory_features'):
        cur.execute('SELECT COUNT(*) FROM trajectory_features')
        features = cur.fetchone()[0]

    paper = 0
    if table_exists(cur, 'paper_trades'):
        cur.execute('SELECT COUNT(*) FROM paper_trades')
        paper = cur.fetchone()[0]

    sweep_count = 0
    if table_exists(cur, 'strategy_sweep'):
        cur.execute('SELECT COUNT(*) FROM strategy_sweep')
        sweep_count = cur.fetchone()[0]

    state_count = 0
    if table_exists(cur, 'official_api_token_state'):
        cur.execute('SELECT COUNT(*) FROM official_api_token_state')
        state_count = cur.fetchone()[0]

    collector_log = latest_collector_log(cur)
    collector_text = 'Collector log: n/a'
    if collector_log:
        log_ts, log_status, log_note = collector_log
        collector_text = f'Collector log: {log_ts} | {log_status} | {(log_note or "")[:300]}'

    text = (
        'Status\n\n'
        f'Finished trajectories: {finished}\n'
        f'Live snapshots: {snapshots}\n'
        f'Official token states: {state_count}\n'
        f'Analysis rows: {features}\n'
        f'Paper simulation trades: {paper}\n'
        f'Sweep rows: {sweep_count}\n'
        f'Last snapshot: {last_snapshot}\n\n'
        f'{collector_text}\n\n'
        'Run order: official_api_collector -> fast_finished_collector -> auto_run.py -> clusters.py'
    )
    await update.message.reply_text(text)


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    lines = ['Summary', '']

    for table_name, label in [
        ('finished_tokens', 'Finished trajectories'),
        ('trajectory_features', 'Analysis rows'),
        ('strategy_sweep', 'Sweep rows'),
        ('live_token_features', 'Live feature rows'),
        ('live_signals', 'Live calls'),
        ('paper_signal_trades', 'Live paper trades'),
    ]:
        if table_exists(cur, table_name):
            cur.execute(f'SELECT COUNT(*) FROM {safe_identifier(table_name)}')
            lines.append(f'{label}: {cur.fetchone()[0]}')

    if table_exists(cur, 'live_signals'):
        ensure_live_signal_columns(cur)
        lines.extend(['', 'Recent calls by side:'])
        cur.execute('''
        SELECT UPPER(COALESCE(side, 'UNKNOWN')) AS side,
               COUNT(*) AS n,
               MAX(confidence_pct) AS max_conf,
               AVG(confidence_pct) AS avg_conf,
               MAX(created_at) AS last_created
        FROM live_signals
        WHERE created_at >= NOW() - interval '24 hours'
          AND COALESCE(signal_status, 'OPEN') <> 'CANCELLED'
          AND COALESCE(reason, '') NOT LIKE '%%CANCELLED_QUALITY_GATE%%'
        GROUP BY UPPER(COALESCE(side, 'UNKNOWN'))
        ORDER BY max_conf DESC NULLS LAST, n DESC
        ''')
        rows = cur.fetchall()
        if rows:
            for side, n, max_conf, avg_conf, last_created in rows:
                lines.append(f'{side}: n={n} max_conf={max_conf or 0}% avg_conf={(avg_conf or 0):.1f}% last={last_created}')
        else:
            lines.append('No calls in last 24h')

    if table_exists(cur, 'paper_account'):
        ensure_paper_schema(cur)
        cur.execute("SELECT balance_usdt, updated_at FROM paper_account WHERE account_key = 'default'")
        row = cur.fetchone()
        if row:
            balance_usdt, updated_at = row
            lines.extend(['', f'Paper account: {fmt_money(balance_usdt)} updated={updated_at}'])

    if table_exists(cur, 'paper_signal_trades'):
        ensure_paper_schema(cur)
        lines.extend(['', 'Paper trades:'])
        cur.execute('''
        SELECT UPPER(COALESCE(side, 'UNKNOWN')) AS side,
               status,
               COUNT(*) AS n,
               AVG(pnl_pct) FILTER (WHERE pnl_pct IS NOT NULL) AS avg_pnl,
               SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) FILTER (WHERE status = 'CLOSED') AS wins
        FROM paper_signal_trades
        GROUP BY UPPER(COALESCE(side, 'UNKNOWN')), status
        ORDER BY side, status
        ''')
        for side, status_name, n, avg_pnl, wins in cur.fetchall():
            win_text = f' wins={wins or 0}' if status_name == 'CLOSED' else ''
            lines.append(f'{side} {status_name}: n={n}{win_text} avg_pnl={fmt_pct(avg_pnl)}')

    if table_exists(cur, 'strategy_sweep'):
        lines.extend(['', 'Best strategies by side:'])
        cur.execute('''
        SELECT DISTINCT ON (UPPER(side))
               UPPER(side) AS side, mode, strategy, trades, winrate, avg_pnl
        FROM strategy_sweep
        WHERE UPPER(side) IN ('LONG', 'SHORT')
        ORDER BY UPPER(side), avg_pnl DESC, winrate DESC, trades DESC
        ''')
        for side, mode, strategy, trades, winrate, avg_pnl in cur.fetchall():
            lines.append(f'{side} {mode} {strategy} n={trades} win={winrate*100:.1f}% avg={avg_pnl:.2f}%')

    await reply_text(update, '\n'.join(lines))


async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    feature_stale = int(os.getenv('LIVE_FEATURES_STALE_SECONDS', '60'))
    signal_stale = int(os.getenv('LIVE_SIGNALS_STALE_SECONDS', '60'))
    min_signal_snapshots = int(os.getenv('LIVE_SIGNALS_MIN_SNAPSHOTS', '3'))
    min_trades = int(os.getenv('LIVE_SIGNALS_MIN_TRADES', '50'))
    min_winrate = float(os.getenv('LIVE_SIGNALS_MIN_WINRATE', '0.55'))
    min_expectancy = float(os.getenv('LIVE_SIGNALS_MIN_EXPECTANCY', '5'))
    long_min_trades = int(os.getenv('LIVE_SIGNALS_LONG_MIN_TRADES', '50'))
    long_min_winrate = float(os.getenv('LIVE_SIGNALS_LONG_MIN_WINRATE', '0.55'))
    long_min_expectancy = float(os.getenv('LIVE_SIGNALS_LONG_MIN_EXPECTANCY', '5'))
    min_median = float(os.getenv('LIVE_SIGNALS_MIN_MEDIAN_PNL', '5'))
    max_worst = float(os.getenv('LIVE_SIGNALS_MAX_WORST_PNL', '-70'))
    max_stop_distance = float(os.getenv('LIVE_SIGNALS_MAX_STOP_DISTANCE_PCT', '120'))
    min_reward_risk = float(os.getenv('LIVE_SIGNALS_MIN_REWARD_RISK', '0.35'))

    lines = ['Health', '']

    if table_exists(cur, 'token_snapshots'):
        cur.execute("SELECT COUNT(*), MAX(ts) FROM token_snapshots WHERE ts >= NOW() - interval '1 minute'")
        snapshots_1m, last_snapshot = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM token_snapshots WHERE ts >= NOW() - interval '5 minutes'")
        snapshots_5m = cur.fetchone()[0]
        cur.execute('''
        SELECT COUNT(*)
        FROM (
            SELECT token_id
            FROM token_snapshots
            WHERE ts >= NOW() - interval '10 minutes'
            GROUP BY token_id
            HAVING COUNT(*) >= 2
        ) t
        ''')
        tokens_2 = cur.fetchone()[0]
        cur.execute('''
        SELECT COUNT(*)
        FROM (
            SELECT token_id
            FROM token_snapshots
            WHERE ts >= NOW() - interval '10 minutes'
            GROUP BY token_id
            HAVING COUNT(*) >= %s
        ) t
        ''', (min_signal_snapshots,))
        tokens_min = cur.fetchone()[0]
        lines.append(f'Snapshots 1m: {snapshots_1m}')
        lines.append(f'Snapshots 5m: {snapshots_5m}')
        lines.append(f'Last snapshot: {last_snapshot}')
        lines.append(f'Tokens with 2 snapshots/10m: {tokens_2}')
        lines.append(f'Tokens with {min_signal_snapshots} snapshots/10m: {tokens_min}')

    if table_exists(cur, 'live_token_features'):
        cur.execute('''
        SELECT COUNT(*)
        FROM live_token_features
        WHERE last_seen >= NOW() - (%s || ' seconds')::interval
        ''', (feature_stale,))
        fresh_features = cur.fetchone()[0]
        cur.execute('SELECT COUNT(*), MAX(updated_at) FROM live_token_features')
        feature_total, feature_updated = cur.fetchone()
        lines.extend([
            '',
            f'Live features fresh {feature_stale}s: {fresh_features}',
            f'Live features total: {feature_total}',
            f'Features last updated: {feature_updated}',
        ])

    if table_exists(cur, 'strategy_sweep'):
        lines.extend([
            '',
            'Quality gates:',
            f'SHORT trades>={min_trades} win>={min_winrate*100:.1f}% avg>={min_expectancy:.2f}%',
            f'LONG trades>={long_min_trades} win>={long_min_winrate*100:.1f}% avg>={long_min_expectancy:.2f}%',
            f'median>={min_median:.2f}% worst>={max_worst:.2f}% stop<={max_stop_distance:.2f}% rr>={min_reward_risk:.2f}',
        ])
        cur.execute('''
        SELECT UPPER(side), COUNT(*)
        FROM strategy_sweep
        WHERE (
            UPPER(side) = 'SHORT'
            AND trades >= %s
            AND winrate >= %s
            AND avg_pnl >= %s
            AND median_pnl >= %s
            AND worst_pnl >= %s
        ) OR (
            UPPER(side) = 'LONG'
            AND trades >= %s
            AND winrate >= %s
            AND avg_pnl >= %s
            AND median_pnl >= %s
            AND worst_pnl >= %s
        )
        GROUP BY UPPER(side)
        ORDER BY UPPER(side)
        ''', (
            min_trades, min_winrate, min_expectancy, min_median, max_worst,
            long_min_trades, long_min_winrate, long_min_expectancy, min_median, max_worst,
        ))
        rows = cur.fetchall()
        lines.extend(['', 'Eligible strategies:'])
        if rows:
            for side, count in rows:
                lines.append(f'{side}: {count}')
        else:
            lines.append('none')

    if table_exists(cur, 'live_token_features') and table_exists(cur, 'strategy_sweep'):
        cur.execute('''
        SELECT UPPER(ss.side), COUNT(*)
        FROM live_token_features lf
        JOIN strategy_sweep ss ON UPPER(ss.mode) = UPPER(lf.mode)
        WHERE lf.last_seen >= NOW() - (%s || ' seconds')::interval
          AND (
              (
                  UPPER(ss.side) = 'SHORT'
                  AND ss.trades >= %s
                  AND ss.winrate >= %s
                  AND ss.avg_pnl >= %s
                  AND ss.median_pnl >= %s
                  AND ss.worst_pnl >= %s
              )
              OR (
                  UPPER(ss.side) = 'LONG'
                  AND ss.trades >= %s
                  AND ss.winrate >= %s
                  AND ss.avg_pnl >= %s
                  AND ss.median_pnl >= %s
                  AND ss.worst_pnl >= %s
              )
          )
        GROUP BY UPPER(ss.side)
        ORDER BY UPPER(ss.side)
        ''', (
            signal_stale,
            min_trades, min_winrate, min_expectancy, min_median, max_worst,
            long_min_trades, long_min_winrate, long_min_expectancy, min_median, max_worst,
        ))
        rows = cur.fetchall()
        lines.extend(['', f'Joined candidates fresh {signal_stale}s:'])
        if rows:
            for side, count in rows:
                lines.append(f'{side}: {count}')
        else:
            lines.append('none')

    if table_exists(cur, 'signal_debug_log'):
        cur.execute('''
        SELECT status, COUNT(*), MAX(ts)
        FROM signal_debug_log
        WHERE ts >= NOW() - interval '30 minutes'
        GROUP BY status
        ORDER BY COUNT(*) DESC
        LIMIT 8
        ''')
        rows = cur.fetchall()
        lines.extend(['', 'Signal skips 30m:'])
        if rows:
            for status_name, count, last_ts in rows:
                lines.append(f'{status_name}: {count} last={last_ts}')
        else:
            lines.append('none')

    if table_exists(cur, 'live_signals'):
        ensure_live_signal_columns(cur)
        cur.execute('''
        SELECT COUNT(*), MAX(created_at)
        FROM live_signals
        WHERE created_at >= NOW() - interval '30 minutes'
          AND COALESCE(signal_status, 'OPEN') <> 'CANCELLED'
          AND COALESCE(reason, '') NOT LIKE '%%CANCELLED_QUALITY_GATE%%'
        ''')
        signal_count, last_signal = cur.fetchone()
        lines.extend(['', f'Signals 30m: {signal_count}', f'Last signal: {last_signal}'])

    if table_exists(cur, 'paper_signal_trades'):
        cur.execute("SELECT status, COUNT(*) FROM paper_signal_trades GROUP BY status ORDER BY status")
        rows = cur.fetchall()
        lines.extend(['', 'Paper trades:'])
        for status_name, count in rows:
            lines.append(f'{status_name}: {count}')

    collector_log = latest_collector_log(cur)
    if collector_log:
        log_ts, log_status, log_note = collector_log
        lines.extend(['', f'Collector: {log_ts} | {log_status} | {(log_note or "")[:500]}'])

    await reply_text(update, '\n'.join(lines))


async def calls_for_side(update: Update, context: ContextTypes.DEFAULT_TYPE, forced_side=None):
    cur = conn.cursor()
    if not table_exists(cur, 'live_signals'):
        await update.message.reply_text('live_signals table does not exist yet.')
        return
    ensure_live_signal_columns(cur)

    limit = arg_limit(context)
    side = forced_side or arg_side(context)
    params = []
    side_sql = ''
    if side:
        side_sql = 'AND UPPER(side) = %s'
        params.append(side)
    params.append(limit)

    cur.execute(f'''
    SELECT ls.token_id, os.name, os.symbol, ls.mode, UPPER(ls.side), ls.confidence_pct,
           ls.current_price, ls.take_profit_price, ls.stop_loss_price, ls.created_at
    FROM live_signals ls
    LEFT JOIN official_api_token_state os ON os.token_id = ls.token_id
    WHERE ls.created_at >= NOW() - interval '24 hours'
      AND COALESCE(ls.signal_status, 'OPEN') <> 'CANCELLED'
      AND COALESCE(ls.reason, '') NOT LIKE '%%CANCELLED_QUALITY_GATE%%'
      {side_sql}
    ORDER BY ls.confidence_pct DESC NULLS LAST,
             ls.historical_avg_pnl DESC NULLS LAST,
             ls.historical_winrate DESC NULLS LAST,
             ls.created_at DESC
    LIMIT %s
    ''', params)
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No recent calls found.')
        return

    lines = [f'Calls {len(rows)}' + (f' {side}' if side else ''), '']
    for row in rows:
        (
            token_id, name, symbol, mode, row_side, confidence_pct,
            current_price, tp, sl, created_at,
        ) = row
        lines.append(
            f'{row_side} {mode} conf {confidence_pct}%\n'
            f'{token_label(name, symbol, token_id)}\n'
            f'Entry {fmt_price(current_price)} | TP {fmt_price(tp)} | SL {fmt_price(sl)}\n'
            f'{token_url(token_id)}'
        )
        lines.append('')

    await reply_text(update, '\n'.join(lines).strip())


async def calls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await calls_for_side(update, context)


async def long_calls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await calls_for_side(update, context, forced_side='LONG')


async def short_calls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await calls_for_side(update, context, forced_side='SHORT')


async def paper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    if not table_exists(cur, 'paper_signal_trades'):
        await update.message.reply_text('paper_signal_trades table does not exist yet.')
        return

    limit = arg_limit(context)
    cur.execute('''
    SELECT pst.signal_id, pst.token_id, os.name, os.symbol, pst.mode, UPPER(pst.side),
           pst.status, pst.entry_price, pst.take_profit_price, pst.stop_loss_price,
           pst.close_price, pst.pnl_pct, pst.close_reason,
           pst.virtual_position_usdt, pst.virtual_pnl_usdt, pst.virtual_balance_after_close,
           COALESCE(os.current_price, ltf.current_price, pst.last_mark_price) AS mark_price,
           pst.last_pnl_pct, pst.last_pnl_usdt, pst.last_marked_at
    FROM paper_signal_trades pst
    LEFT JOIN official_api_token_state os ON os.token_id = pst.token_id
    LEFT JOIN live_token_features ltf ON ltf.token_id = pst.token_id
    ORDER BY COALESCE(pst.closed_at, pst.opened_at) DESC NULLS LAST, pst.id DESC
    LIMIT %s
    ''', (limit,))
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No live paper trades yet.')
        return

    lines = [f'Paper trades {len(rows)}', '']
    for row in rows:
        (
            signal_id, token_id, name, symbol, mode, side, status_name,
            entry, tp, sl, close, pnl, close_reason,
            virtual_position, virtual_pnl, virtual_balance_after,
            mark_price, last_pnl_pct, last_pnl_usdt, last_marked_at,
        ) = row
        if close is not None:
            close_text = f'Close {fmt_price(close)} | PnL {fmt_pct(pnl)} ({fmt_money(virtual_pnl)})'
        elif last_pnl_pct is not None:
            close_text = f'Now {fmt_price(mark_price)} | uPnL {fmt_pct(last_pnl_pct)} ({fmt_money(last_pnl_usdt)})'
        else:
            close_text = f'Now {fmt_price(mark_price)} | uPnL n/a'
        lines.append(
            f'#{signal_id} {status_name} {side} {mode}\n'
            f'{token_label(name, symbol, token_id)}\n'
            f'Entry {fmt_price(entry)} | TP {fmt_price(tp)} | SL {fmt_price(sl)}\n'
            f'{close_text} | Size {fmt_money(virtual_position)}\n'
            f'{token_url(token_id)}'
        )
        lines.append('')

    await reply_text(update, '\n'.join(lines).strip())


async def paper_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    snapshot = paper_account_snapshot(cur)
    auto_open = paper_bool(cur, 'auto_open', PAPER_AUTO_OPEN)
    trade_size = float(paper_setting(cur, 'trade_size_usdt', str(PAPER_TRADE_SIZE_USDT)))
    max_open = int(float(paper_setting(cur, 'max_open_trades', str(PAPER_MAX_OPEN_TRADES))))
    max_position_pct = float(paper_setting(cur, 'max_position_pct', str(PAPER_MAX_POSITION_PCT)))
    effective_size = trade_size
    if max_position_pct > 0:
        effective_size = min(effective_size, snapshot['equity'] * max_position_pct / 100.0)
    effective_size = min(effective_size, snapshot['free'])
    cur.execute("SELECT COUNT(*), COALESCE(SUM(virtual_pnl_usdt), 0) FROM paper_signal_trades WHERE status = 'CLOSED'")
    closed_count, closed_pnl = cur.fetchone()

    text = (
        'Paper account\n\n'
        f'Auto: {"ON" if auto_open else "OFF"}\n'
        f'Free: {fmt_money(snapshot["free"])}\n'
        f'Reserved: {fmt_money(snapshot["reserved"])}\n'
        f'Unrealized: {fmt_money(snapshot["unrealized"])}\n'
        f'Equity: {fmt_money(snapshot["equity"])}\n'
        f'Next size: {fmt_money(effective_size)}\n'
        f'Open: {snapshot["open_count"] or 0}/{max_open}\n'
        f'Closed: {closed_count or 0} | PnL: {fmt_money(closed_pnl)}'
    )
    await update.message.reply_text(text)


async def paper_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    if not context.args:
        await paper_account(update, context)
        return

    action = context.args[0].lower()
    if action in ('add', 'remove', 'set'):
        amount = None
        for arg in context.args[1:]:
            try:
                amount = float(arg.replace('$', '').replace(',', ''))
                break
            except ValueError:
                continue
    else:
        action = 'set'
        amount = parse_float_arg(context)

    if amount is None:
        await update.message.reply_text('Use /balance add 100, /balance remove 50, or /balance set 1000.')
        return
    if amount < 0:
        await update.message.reply_text('Amount must be >= 0.')
        return

    current = account_balance(cur)
    if action == 'add':
        new_balance = update_account_balance(cur, amount)
        verb = 'added'
    elif action == 'remove':
        if amount > current:
            await update.message.reply_text(f'Not enough free balance. Free: {fmt_money(current)}')
            return
        new_balance = update_account_balance(cur, -amount)
        verb = 'removed'
    else:
        new_balance = amount
        verb = 'set'
        cur.execute('''
        INSERT INTO paper_account(account_key, balance_usdt, updated_at)
        VALUES ('default', %s, %s)
        ON CONFLICT(account_key) DO UPDATE SET
            balance_usdt = EXCLUDED.balance_usdt,
            updated_at = EXCLUDED.updated_at
        ''', (new_balance, utcnow()))

    snapshot = paper_account_snapshot(cur)
    await update.message.reply_text(
        f'Balance {verb}: {fmt_money(amount)}\n'
        f'Free: {fmt_money(snapshot["free"])}\n'
        f'Reserved: {fmt_money(snapshot["reserved"])}\n'
        f'Equity: {fmt_money(snapshot["equity"])}'
    )


async def paper_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    amount = parse_float_arg(context)
    if amount is None:
        current = paper_setting(cur, 'trade_size_usdt', str(PAPER_TRADE_SIZE_USDT))
        await update.message.reply_text(f'Current max paper trade size: {fmt_money(current)}')
        return
    if amount <= 0:
        await update.message.reply_text('Trade size must be > 0.')
        return
    set_paper_setting(cur, 'trade_size_usdt', amount)
    await update.message.reply_text(f'Max paper trade size set to {fmt_money(amount)}.')


async def paper_auto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    if not context.args:
        auto_open = paper_bool(cur, 'auto_open', PAPER_AUTO_OPEN)
        await update.message.reply_text(f'Paper auto open is {"ON" if auto_open else "OFF"}. Use /auto on or /auto off.')
        return
    value = context.args[0].lower()
    if value not in ('on', 'off', 'true', 'false', '1', '0', 'yes', 'no'):
        await update.message.reply_text('Use /auto on or /auto off.')
        return
    enabled = value in ('on', 'true', '1', 'yes')
    set_paper_setting(cur, 'auto_open', 'true' if enabled else 'false')
    await update.message.reply_text(f'Paper auto open set to {"ON" if enabled else "OFF"}.')


async def paper_limits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    numbers = []
    for arg in context.args:
        try:
            numbers.append(float(arg.replace('%', '').replace(',', '')))
        except ValueError:
            continue

    if not numbers:
        max_open = paper_setting(cur, 'max_open_trades', str(PAPER_MAX_OPEN_TRADES))
        max_pct = paper_setting(cur, 'max_position_pct', str(PAPER_MAX_POSITION_PCT))
        await update.message.reply_text(
            f'Current paper limits: max_open={max_open}, max_position_pct={float(max_pct):.2f}%.\n'
            'Use /paper_limits 10 10 to set max open trades and max position percent.'
        )
        return

    max_open = int(numbers[0])
    if max_open < 0:
        await update.message.reply_text('max_open must be >= 0.')
        return
    set_paper_setting(cur, 'max_open_trades', max_open)

    message = f'Max open paper trades set to {max_open}.'
    if len(numbers) >= 2:
        max_pct = numbers[1]
        if max_pct <= 0:
            await update.message.reply_text('max_position_pct must be > 0.')
            return
        set_paper_setting(cur, 'max_position_pct', max_pct)
        message += f' Max position pct set to {max_pct:.2f}%.'

    await update.message.reply_text(message)


async def paper_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    days = int(parse_float_arg(context) or 30)
    days = max(1, min(365, days))

    cur.execute('''
    SELECT
        COUNT(*) AS closed,
        COUNT(*) FILTER (WHERE pnl_pct > 0) AS wins,
        COUNT(*) FILTER (WHERE pnl_pct < 0) AS losses,
        COUNT(*) FILTER (WHERE pnl_pct = 0) AS flat,
        AVG(pnl_pct) AS avg_pnl,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pnl_pct) AS median_pnl,
        MIN(pnl_pct) AS worst_pnl,
        MAX(pnl_pct) AS best_pnl,
        COALESCE(SUM(virtual_pnl_usdt), 0) AS virtual_pnl
    FROM paper_signal_trades
    WHERE status = 'CLOSED'
      AND closed_at >= NOW() - (%s || ' days')::interval
    ''', (days,))
    closed, wins, losses, flat, avg_pnl, median_pnl, worst_pnl, best_pnl, virtual_pnl = cur.fetchone()
    winrate_text = f'{(wins / closed * 100.0):.1f}% ({wins or 0}W / {losses or 0}L)' if closed else 'n/a (no closed trades yet)'
    snapshot = paper_account_snapshot(cur)

    lines = [
        f'Paper stats {days}d',
        '',
        f'Free: {fmt_money(snapshot["free"])}',
        f'Reserved: {fmt_money(snapshot["reserved"])}',
        f'Unrealized: {fmt_money(snapshot["unrealized"])}',
        f'Equity: {fmt_money(snapshot["equity"])}',
        f'Open trades: {snapshot["open_count"] or 0}',
        f'Closed: {closed or 0}',
        f'Winrate: {winrate_text}',
        f'Avg: {fmt_pct(avg_pnl)} | Best: {fmt_pct(best_pnl)} | Worst: {fmt_pct(worst_pnl)}',
        f'Realized PnL: {fmt_money(virtual_pnl)}',
    ]

    cur.execute('''
    SELECT UPPER(COALESCE(side, 'UNKNOWN')) AS side,
           COUNT(*) AS closed,
           COUNT(*) FILTER (WHERE pnl_pct > 0) AS wins,
           AVG(pnl_pct) AS avg_pnl,
           COALESCE(SUM(virtual_pnl_usdt), 0) AS virtual_pnl
    FROM paper_signal_trades
    WHERE status = 'CLOSED'
      AND closed_at >= NOW() - (%s || ' days')::interval
    GROUP BY UPPER(COALESCE(side, 'UNKNOWN'))
    ORDER BY closed DESC, side
    ''', (days,))
    side_rows = cur.fetchall()
    if side_rows:
        lines.extend(['', 'By side:'])
        for side, side_closed, side_wins, side_avg, side_virtual in side_rows:
            side_winrate = (side_wins / side_closed * 100.0) if side_closed else 0
            lines.append(f'{side}: n={side_closed} win={side_winrate:.1f}% avg={fmt_pct(side_avg)} pnl={fmt_money(side_virtual)}')

    cur.execute('''
    SELECT close_reason, COUNT(*) AS n
    FROM paper_signal_trades
    WHERE status = 'CLOSED'
      AND closed_at >= NOW() - (%s || ' days')::interval
    GROUP BY close_reason
    ORDER BY n DESC, close_reason
    ''', (days,))
    reason_rows = cur.fetchall()
    if reason_rows:
        lines.extend(['', 'Reasons:'])
        for close_reason, count in reason_rows:
            lines.append(f'{close_reason or "n/a"}: {count}')

    await reply_text(update, '\n'.join(lines))


async def smart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    ensure_paper_schema(cur)
    if not table_exists(cur, 'live_signals'):
        await update.message.reply_text('No live signals yet.')
        return

    days = int(os.getenv('LIVE_SIGNALS_ADAPTIVE_LOOKBACK_DAYS', '30'))
    min_closed = int(os.getenv('LIVE_SIGNALS_ADAPTIVE_MIN_CLOSED_TRADES', '5'))
    bad_win = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_DISABLE_WINRATE', '0.35'))
    bad_avg = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_DISABLE_AVG_PNL', '-15'))
    boost_win = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_BOOST_WINRATE', '0.60'))
    boost_avg = float(os.getenv('LIVE_SIGNALS_ADAPTIVE_BOOST_AVG_PNL', '5'))

    cur.execute('''
    SELECT
        UPPER(ls.side) AS side,
        UPPER(ls.mode) AS mode,
        ls.matched_strategy,
        COUNT(*) AS closed,
        COUNT(*) FILTER (WHERE pst.pnl_pct > 0) AS wins,
        AVG(pst.pnl_pct) AS avg_pnl
    FROM paper_signal_trades pst
    JOIN live_signals ls ON ls.id = pst.signal_id
    WHERE pst.status = 'CLOSED'
      AND pst.closed_at >= NOW() - (%s || ' days')::interval
    GROUP BY UPPER(ls.side), UPPER(ls.mode), ls.matched_strategy
    ORDER BY closed DESC, AVG(pst.pnl_pct) DESC NULLS LAST
    LIMIT 12
    ''', (days,))
    rows = cur.fetchall()

    lines = [
        'Smart learning',
        '',
        f'Looks at last {days}d closed paper trades.',
        f'Blocks after {min_closed}+ closes if win < {bad_win*100:.0f}% or avg < {bad_avg:.1f}%.',
        f'Boosts if win >= {boost_win*100:.0f}% and avg >= {boost_avg:.1f}%.',
        '',
    ]

    if not rows:
        lines.append(f'No closed paper trades yet. It starts learning after {min_closed} closes.')
    else:
        for side, mode, strategy, closed, wins, avg_pnl in rows:
            winrate = (wins / closed * 100.0) if closed else 0
            avg = float(avg_pnl or 0)
            if closed >= min_closed and (winrate < bad_win * 100.0 or avg < bad_avg):
                state = 'BLOCK'
            elif closed >= min_closed and winrate >= boost_win * 100.0 and avg >= boost_avg:
                state = 'BOOST'
            elif closed >= min_closed:
                state = 'OK'
            else:
                state = 'LEARN'
            lines.append(f'{state} {side} {mode} n={closed} win={winrate:.1f}% avg={avg:.2f}%')
            lines.append(str(strategy or 'strategy n/a'))

    await reply_text(update, '\n'.join(lines))


async def sweep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    if not table_exists(cur, 'strategy_sweep'):
        await update.message.reply_text('strategy_sweep table does not exist yet. Run auto_run.py first.')
        return

    limit = arg_limit(context, default=20)
    side = arg_side(context)
    params = []
    side_sql = ''
    if side:
        side_sql = 'AND UPPER(side) = %s'
        params.append(side)
    params.append(limit)

    cur.execute(f'''
    SELECT mode, UPPER(side), strategy, trades, winrate, avg_pnl,
           median_pnl, worst_pnl, best_pnl
    FROM strategy_sweep
    WHERE UPPER(side) IN ('LONG', 'SHORT')
      {side_sql}
    ORDER BY avg_pnl DESC, winrate DESC, trades DESC
    LIMIT %s
    ''', params)
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No sweep rows match.')
        return

    lines = [f'Sweep top {len(rows)}' + (f' {side}' if side else ''), '']
    for mode, row_side, strategy, trades, winrate, avg_pnl, med, worst, best in rows:
        lines.append(
            f'{row_side} {mode} {strategy}\n'
            f'n={trades} win={winrate*100:.1f}% avg={avg_pnl:.2f}% med={med:.2f}% worst={worst:.2f}% best={best:.2f}%'
        )
        lines.append('')

    await reply_text(update, '\n'.join(lines).strip())


async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = conn.cursor()
    await update.message.reply_text('Creating CSV backup ZIP...')
    zip_buffer = io.BytesIO()
    manifest_lines = [
        'caput backup',
        f'created_utc={datetime.now(timezone.utc).isoformat()}',
        f'backup_max_rows_per_table={BACKUP_MAX_ROWS_PER_TABLE}',
        f'backup_include_snapshots={BACKUP_INCLUDE_SNAPSHOTS}',
        '',
        'tables:',
    ]
    with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        for table_name in BACKUP_TABLES:
            try:
                row_count = write_table_csv_to_zip(cur, zf, table_name)
                if row_count:
                    manifest_lines.append(f'- {table_name}: {row_count} rows')
            except Exception as exc:
                manifest_lines.append(f'- {table_name}: ERROR {exc}')
        zf.writestr('MANIFEST.txt', '\n'.join(manifest_lines) + '\n')
    zip_buffer.seek(0)
    filename = f'caput_backup_{utc_stamp()}.zip'
    await update.message.reply_document(document=zip_buffer, filename=filename)


async def run_pg_dump(update: Update, table_names=None, label='full'):
    await update.message.reply_text(f'Creating PostgreSQL {label} pg_dump backup...')
    selected_tables = existing_tables(table_names) if table_names else None
    if table_names and not selected_tables:
        await update.message.reply_text('No requested tables exist yet, nothing to dump.')
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        dump_path = os.path.join(tmpdir, 'caput_pg_dump.dump')
        dump_cmd = [
            'pg_dump', '--format=custom', '--compress=9', '--no-owner', '--no-privileges',
            DATABASE_URL, '-f', dump_path,
        ]
        if selected_tables:
            for table_name in selected_tables:
                dump_cmd.extend(['--table', f'public.{safe_identifier(table_name)}'])

        try:
            dump_proc = subprocess.run(dump_cmd, capture_output=True, text=True, timeout=PG_DUMP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            await update.message.reply_text(
                f'pg_dump timed out after {PG_DUMP_TIMEOUT_SECONDS}s. Use /pg_dump_essential or increase PG_DUMP_TIMEOUT_SECONDS.'
            )
            return

        if dump_proc.returncode != 0:
            await update.message.reply_text(f'pg_dump failed:\n\n{dump_proc.stderr[-3000:]}')
            return

        filename = f'caput_pg_dump_{label}_{utc_stamp()}.dump'
        size_mb = os.path.getsize(dump_path) / 1024 / 1024
        table_note = ''
        if selected_tables:
            table_note = '\nTables: ' + ', '.join(selected_tables)

        if size_mb > TELEGRAM_MAX_UPLOAD_MB:
            await update.message.reply_text(
                f'pg_dump ready but too large for Telegram: {size_mb:.2f} MB.\n'
                f'Configured Telegram limit: {TELEGRAM_MAX_UPLOAD_MB:.2f} MB.\n'
                f'Use /pg_dump_essential for a smaller backup or store full backups outside Telegram.{table_note}'
            )
            return

        await update.message.reply_text(f'pg_dump ready: {size_mb:.2f} MB. Uploading...{table_note}')
        try:
            with open(dump_path, 'rb') as f:
                await update.message.reply_document(document=f, filename=filename)
        except Exception as exc:
            await update.message.reply_text(f'Telegram upload failed: {exc}')


async def pg_dump_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_pg_dump(update, table_names=None, label='full')


async def pg_dump_core(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_pg_dump(update, table_names=PG_DUMP_CORE_TABLES, label='core')


async def pg_dump_essential(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_pg_dump(update, table_names=PG_DUMP_ESSENTIAL_TABLES, label='essential')


app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler('start', start))
app.add_handler(CommandHandler('status', status))
app.add_handler(CommandHandler('stats', paper_stats))
app.add_handler(CommandHandler('summary', summary))
app.add_handler(CommandHandler('health', health))
app.add_handler(CommandHandler('calls', calls))
app.add_handler(CommandHandler('long', long_calls))
app.add_handler(CommandHandler('short', short_calls))
app.add_handler(CommandHandler('paper', paper))
app.add_handler(CommandHandler('paper_account', paper_account))
app.add_handler(CommandHandler('paper_balance', paper_balance))
app.add_handler(CommandHandler('balance', paper_balance))
app.add_handler(CommandHandler('paper_size', paper_size))
app.add_handler(CommandHandler('paper_limits', paper_limits))
app.add_handler(CommandHandler('paper_auto', paper_auto))
app.add_handler(CommandHandler('auto', paper_auto))
app.add_handler(CommandHandler('paper_stats', paper_stats))
app.add_handler(CommandHandler('smart', smart))
app.add_handler(CommandHandler('sweep', sweep))
app.add_handler(CommandHandler('backup', backup))
app.add_handler(CommandHandler('pg_dump', pg_dump_backup))
app.add_handler(CommandHandler('pg_dump_core', pg_dump_core))
app.add_handler(CommandHandler('pg_dump_essential', pg_dump_essential))
app.run_polling()
