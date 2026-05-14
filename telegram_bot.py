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


async def reply_text(update, text):
    for start in range(0, len(text), 3900):
        await update.message.reply_text(text[start:start + 3900])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Caput bot online\n\n'
        '/status - collector and dataset status\n'
        '/summary - compact live and strategy summary\n'
        '/health - pipeline freshness and skip diagnostics\n'
        '/calls [long|short] [limit] - best recent live calls\n'
        '/paper [limit] - latest live paper trades\n'
        '/sweep [long|short] [limit] - best historical strategies\n'
        '/backup - export DB CSV ZIP\n'
        '/pg_dump_essential - smallest PostgreSQL dump, best for Telegram\n'
        '/pg_dump_core - PostgreSQL dump without token_snapshots/logs\n'
        '/pg_dump - full PostgreSQL dump, can be too large for Telegram'
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
        cur.execute("SELECT balance_usdt, updated_at FROM paper_account WHERE account_key = 'default'")
        row = cur.fetchone()
        if row:
            balance_usdt, updated_at = row
            lines.extend(['', f'Paper account: {fmt_money(balance_usdt)} updated={updated_at}'])

    if table_exists(cur, 'paper_signal_trades'):
        lines.extend(['', 'Paper trades:'])
        cur.execute('''
        SELECT UPPER(COALESCE(side, 'UNKNOWN')) AS side,
               status,
               COUNT(*) AS n,
               AVG(pnl_pct) FILTER (WHERE pnl_pct IS NOT NULL) AS avg_pnl
        FROM paper_signal_trades
        GROUP BY UPPER(COALESCE(side, 'UNKNOWN')), status
        ORDER BY side, status
        ''')
        for side, status_name, n, avg_pnl in cur.fetchall():
            lines.append(f'{side} {status_name}: n={n} avg_pnl={fmt_pct(avg_pnl)}')

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
        ) OR (
            UPPER(side) = 'LONG'
            AND trades >= %s
            AND winrate >= %s
            AND avg_pnl >= %s
        )
        GROUP BY UPPER(side)
        ORDER BY UPPER(side)
        ''', (min_trades, min_winrate, min_expectancy, long_min_trades, long_min_winrate, long_min_expectancy))
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
              )
              OR (
                  UPPER(ss.side) = 'LONG'
                  AND ss.trades >= %s
                  AND ss.winrate >= %s
                  AND ss.avg_pnl >= %s
              )
          )
        GROUP BY UPPER(ss.side)
        ORDER BY UPPER(ss.side)
        ''', (
            signal_stale,
            min_trades, min_winrate, min_expectancy,
            long_min_trades, long_min_winrate, long_min_expectancy,
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

    has_risk = column_exists(cur, 'live_signals', 'reward_risk')
    risk_select = (
        'liquidation_price, reward_pct, risk_pct, reward_risk,'
        if has_risk else
        'NULL::double precision AS liquidation_price, NULL::double precision AS reward_pct, '
        'NULL::double precision AS risk_pct, NULL::double precision AS reward_risk,'
    )
    cur.execute(f'''
    SELECT token_id, mode, UPPER(side), confidence_pct, current_price,
           take_profit_price, stop_loss_price, matched_strategy,
           {risk_select}
           historical_trades, historical_winrate, historical_avg_pnl,
           cluster_name, cluster_risk, reversal_from_peak_pct, created_at
    FROM live_signals
    WHERE created_at >= NOW() - interval '24 hours'
      AND COALESCE(signal_status, 'OPEN') <> 'CANCELLED'
      AND COALESCE(reason, '') NOT LIKE '%%CANCELLED_QUALITY_GATE%%'
      {side_sql}
    ORDER BY confidence_pct DESC NULLS LAST,
             historical_avg_pnl DESC NULLS LAST,
             historical_winrate DESC NULLS LAST,
             created_at DESC
    LIMIT %s
    ''', params)
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No recent calls found.')
        return

    lines = [f'Calls top {len(rows)}' + (f' {side}' if side else ''), '']
    for row in rows:
        (
            token_id, mode, row_side, confidence_pct, current_price,
            tp, sl, strategy, liquidation_price, reward_pct, risk_pct, reward_risk,
            trades, winrate, avg_pnl,
            cluster_name, cluster_risk, setup_move, created_at,
        ) = row
        setup_label = 'drawdown' if row_side == 'LONG' else 'reversal'
        lines.append(
            f'{row_side} {mode} conf={confidence_pct}% avg={avg_pnl:.2f}% win={winrate*100:.1f}% n={trades}\n'
            f'{token_id} price={fmt_price(current_price)} tp={fmt_price(tp)} sl={fmt_price(sl)} liq={fmt_price(liquidation_price)}\n'
            f'rr={(reward_risk or 0):.2f} reward={(reward_pct or 0):.2f}% risk={(risk_pct or 0):.2f}% {setup_label}={(setup_move or 0):.2f}%\n'
            f'{strategy} | {cluster_name or "n/a"}/{cluster_risk or "n/a"} | {created_at}'
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
    if not table_exists(cur, 'paper_signal_trades'):
        await update.message.reply_text('paper_signal_trades table does not exist yet.')
        return

    limit = arg_limit(context)
    has_virtual = column_exists(cur, 'paper_signal_trades', 'virtual_position_usdt')
    has_risk = column_exists(cur, 'paper_signal_trades', 'reward_risk')
    risk_select = (
        'liquidation_price, reward_pct, risk_pct, reward_risk,'
        if has_risk else
        'NULL::double precision AS liquidation_price, NULL::double precision AS reward_pct, '
        'NULL::double precision AS risk_pct, NULL::double precision AS reward_risk,'
    )
    if has_virtual:
        cur.execute(f'''
        SELECT token_id, mode, UPPER(side), status, confidence_pct,
               entry_price, take_profit_price, stop_loss_price, close_price,
               pnl_pct, close_reason, opened_at, closed_at,
               {risk_select}
               virtual_position_usdt, virtual_pnl_usdt, virtual_balance_after_close
        FROM paper_signal_trades
        ORDER BY COALESCE(closed_at, opened_at) DESC NULLS LAST, id DESC
        LIMIT %s
        ''', (limit,))
    else:
        cur.execute(f'''
        SELECT token_id, mode, UPPER(side), status, confidence_pct,
               entry_price, take_profit_price, stop_loss_price, close_price,
               pnl_pct, close_reason, opened_at, closed_at,
               {risk_select}
               NULL, NULL, NULL
        FROM paper_signal_trades
        ORDER BY COALESCE(closed_at, opened_at) DESC NULLS LAST, id DESC
        LIMIT %s
        ''', (limit,))
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text('No live paper trades yet.')
        return

    lines = [f'Latest paper trades top {len(rows)}', '']
    for row in rows:
        (
            token_id, mode, side, status_name, confidence_pct,
            entry, tp, sl, close, pnl, close_reason, opened_at, closed_at,
            liquidation_price, reward_pct, risk_pct, reward_risk,
            virtual_position, virtual_pnl, virtual_balance_after,
        ) = row
        lines.append(
            f'{side} {mode} {status_name} conf={confidence_pct}% pnl={fmt_pct(pnl)} reason={close_reason or "open"}\n'
            f'virtual_position={fmt_money(virtual_position)} virtual_pnl={fmt_money(virtual_pnl)} balance_after={fmt_money(virtual_balance_after)}\n'
            f'{token_id} entry={fmt_price(entry)} tp={fmt_price(tp)} sl={fmt_price(sl)} liq={fmt_price(liquidation_price)} close={fmt_price(close)}\n'
            f'rr={(reward_risk or 0):.2f} reward={(reward_pct or 0):.2f}% risk={(risk_pct or 0):.2f}%\n'
            f'opened={opened_at} closed={closed_at}'
        )
        lines.append('')

    await reply_text(update, '\n'.join(lines).strip())


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
app.add_handler(CommandHandler('stats', status))
app.add_handler(CommandHandler('summary', summary))
app.add_handler(CommandHandler('health', health))
app.add_handler(CommandHandler('calls', calls))
app.add_handler(CommandHandler('long', long_calls))
app.add_handler(CommandHandler('short', short_calls))
app.add_handler(CommandHandler('paper', paper))
app.add_handler(CommandHandler('sweep', sweep))
app.add_handler(CommandHandler('backup', backup))
app.add_handler(CommandHandler('pg_dump', pg_dump_backup))
app.add_handler(CommandHandler('pg_dump_core', pg_dump_core))
app.add_handler(CommandHandler('pg_dump_essential', pg_dump_essential))
app.run_polling()
