import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')
SIGNAL_CHAT_ID = os.getenv('TELEGRAM_SIGNAL_CHAT_ID')
INTERVAL_SECONDS = float(os.getenv('SIGNAL_BROADCASTER_INTERVAL_SECONDS', '10'))
MAX_SIGNAL_AGE_MINUTES = int(os.getenv('SIGNAL_BROADCASTER_MAX_SIGNAL_AGE_MINUTES', '60'))
PAPER_START_BALANCE_USDT = float(os.getenv('PAPER_START_BALANCE_USDT', '1000'))
PAPER_TRADE_SIZE_USDT = float(os.getenv('PAPER_TRADE_SIZE_USDT', '100'))
PAPER_AUTO_OPEN = os.getenv('PAPER_AUTO_OPEN', 'true').lower() in ('1', 'true', 'yes', 'on')
PAPER_MAX_OPEN_TRADES = int(os.getenv('PAPER_MAX_OPEN_TRADES', '10'))
PAPER_MAX_POSITION_PCT = float(os.getenv('PAPER_MAX_POSITION_PCT', '10'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')
if not BOT_TOKEN:
    raise SystemExit('BOT_TOKEN is missing')
if not SIGNAL_CHAT_ID:
    raise SystemExit('TELEGRAM_SIGNAL_CHAT_ID is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


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
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS sent_to_telegram BOOLEAN DEFAULT FALSE',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_name TEXT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_risk TEXT',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reversal_from_peak_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS max_pump_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS live_snapshots INT',
        "ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS signal_status TEXT DEFAULT 'OPEN'",
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS liquidation_price DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reward_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS risk_pct DOUBLE PRECISION',
        'ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reward_risk DOUBLE PRECISION',
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
    ])

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
    reserve_existing_open_positions()


def paper_setting(key, default):
    cur.execute('SELECT setting_value FROM paper_settings WHERE setting_key = %s', (key,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else default


def paper_setting_float(key, default):
    try:
        return float(paper_setting(key, str(default)))
    except (TypeError, ValueError):
        return float(default)


def paper_setting_int(key, default):
    try:
        return int(float(paper_setting(key, str(default))))
    except (TypeError, ValueError):
        return int(default)


def paper_auto_open_enabled():
    return str(paper_setting('auto_open', 'true' if PAPER_AUTO_OPEN else 'false')).lower() in ('1', 'true', 'yes', 'on')


def configured_trade_size():
    return max(0.0, paper_setting_float('trade_size_usdt', PAPER_TRADE_SIZE_USDT))


def max_open_trades():
    return max(0, paper_setting_int('max_open_trades', PAPER_MAX_OPEN_TRADES))


def reserved_capital():
    cur.execute('''
    SELECT COALESCE(SUM(virtual_position_usdt), 0)
    FROM paper_signal_trades
    WHERE status = 'OPEN'
      AND COALESCE(capital_reserved, false) = true
    ''')
    return float(cur.fetchone()[0] or 0)


def effective_trade_size(balance=None):
    free_balance = account_balance() if balance is None else float(balance)
    equity_for_sizing = free_balance + reserved_capital()
    size = configured_trade_size()
    max_pct = paper_setting_float('max_position_pct', PAPER_MAX_POSITION_PCT)
    if max_pct > 0:
        size = min(size, max(0.0, equity_for_sizing * max_pct / 100.0))
    size = min(size, free_balance)
    return max(0.0, size)


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


def account_balance():
    cur.execute("SELECT balance_usdt FROM paper_account WHERE account_key = 'default'")
    row = cur.fetchone()
    if row:
        return float(row[0])
    cur.execute('''
    INSERT INTO paper_account(account_key, balance_usdt, updated_at)
    VALUES ('default', %s, %s)
    RETURNING balance_usdt
    ''', (PAPER_START_BALANCE_USDT, utcnow()))
    return float(cur.fetchone()[0])


def reserve_existing_open_positions():
    cur.execute('''
    SELECT id, COALESCE(virtual_position_usdt, %s) AS position_usdt
    FROM paper_signal_trades
    WHERE status = 'OPEN'
      AND COALESCE(capital_reserved, false) = false
    ORDER BY id ASC
    ''', (PAPER_TRADE_SIZE_USDT,))
    rows = cur.fetchall()
    if not rows:
        return 0

    reserved = 0
    for trade_id, position_usdt in rows:
        position_usdt = float(position_usdt or 0)
        if position_usdt <= 0:
            continue
        balance = account_balance()
        if balance < position_usdt:
            break
        balance_after = update_account_balance(-position_usdt)
        cur.execute('''
        UPDATE paper_signal_trades
        SET capital_reserved = true,
            virtual_position_usdt = %s,
            virtual_balance_at_open = %s
        WHERE id = %s
        ''', (position_usdt, balance_after, trade_id))
        reserved += 1
    if reserved:
        print(f'reserved existing paper trades: {reserved}', flush=True)
    return reserved


def fill_missing_virtual_open_fields():
    balance = account_balance()
    position_size = effective_trade_size(balance)
    cur.execute('''
    UPDATE paper_signal_trades
    SET virtual_position_usdt = COALESCE(virtual_position_usdt, %s),
        virtual_balance_at_open = COALESCE(virtual_balance_at_open, %s)
    WHERE status = 'OPEN'
      AND (virtual_position_usdt IS NULL OR virtual_balance_at_open IS NULL)
    ''', (position_size, balance))


def update_account_balance(delta_usdt):
    balance = account_balance()
    new_balance = balance + delta_usdt
    cur.execute('''
    UPDATE paper_account
    SET balance_usdt = %s, updated_at = %s
    WHERE account_key = 'default'
    ''', (new_balance, utcnow()))
    return new_balance


def open_missing_paper_trades():
    if not paper_auto_open_enabled():
        return 0

    cur.execute("SELECT COUNT(*) FROM paper_signal_trades WHERE status = 'OPEN'")
    open_count = cur.fetchone()[0]
    slots = max_open_trades() - int(open_count or 0)
    if slots <= 0:
        return 0

    cur.execute('''
    SELECT
        ls.id, ls.token_id, ls.mode, ls.side, ls.confidence_pct,
        ls.current_price, ls.take_profit_price, ls.stop_loss_price, ls.max_leverage,
        ls.liquidation_price, ls.reward_pct, ls.risk_pct, ls.reward_risk
    FROM live_signals ls
    LEFT JOIN paper_signal_trades pst ON pst.signal_id = ls.id
    WHERE pst.id IS NULL
      AND COALESCE(ls.signal_status, 'OPEN') <> 'CANCELLED'
      AND COALESCE(ls.reason, '') NOT LIKE '%%CANCELLED_QUALITY_GATE%%'
      AND ls.created_at >= NOW() - (%s || ' minutes')::interval
    ORDER BY ls.confidence_pct DESC NULLS LAST,
             ls.historical_avg_pnl DESC NULLS LAST,
             ls.created_at ASC,
             ls.id ASC
    LIMIT %s
    ''', (MAX_SIGNAL_AGE_MINUTES, slots))
    rows = cur.fetchall()

    opened = 0
    for row in rows:
        (
            signal_id, token_id, mode, side, confidence_pct,
            entry_price, tp, sl, max_leverage,
            liquidation_price, reward_pct, risk_pct, reward_risk,
        ) = row
        balance = account_balance()
        position_size = effective_trade_size(balance)
        if position_size <= 0 or balance < position_size:
            break
        balance_after = update_account_balance(-position_size)
        cur.execute('''
        INSERT INTO paper_signal_trades(
            signal_id, token_id, mode, side, status, confidence_pct,
            entry_price, take_profit_price, stop_loss_price, max_leverage, opened_at,
            liquidation_price, reward_pct, risk_pct, reward_risk,
            virtual_position_usdt, virtual_balance_at_open, capital_reserved
        ) VALUES (%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true)
        ON CONFLICT(signal_id) DO NOTHING
        RETURNING id
        ''', (
            signal_id, token_id, mode, side, confidence_pct,
            entry_price, tp, sl, max_leverage, utcnow(),
            liquidation_price, reward_pct, risk_pct, reward_risk,
            position_size, balance_after,
        ))
        if cur.fetchone():
            opened += 1
        else:
            update_account_balance(position_size)
    return opened


def tg_send(text):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': SIGNAL_CHAT_ID,
        'text': text,
        'disable_web_page_preview': False,
    }
    response = requests.post(url, json=payload, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f'Telegram send failed {response.status_code}: {response.text[:500]}')
    data = response.json()
    return data.get('result', {}).get('message_id')


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


def fmt_liquidation(value, leverage):
    if value is not None:
        return fmt_price(value)
    if leverage is not None and float(leverage) <= 1:
        return 'n/a (x1)'
    return 'n/a'


def token_label(name, symbol, token_id):
    if symbol and name:
        return f'{symbol} ({name})'
    if symbol:
        return str(symbol)
    if name:
        return str(name)
    return f'Token {token_id}'


def signal_message(row):
    (
        signal_id, token_id, token_name, token_symbol, mode, side, confidence_pct,
        current_price, entry_low, entry_high, take_profit_price, stop_loss_price,
        max_leverage, matched_strategy, historical_trades, historical_winrate,
        historical_avg_pnl, historical_median_pnl, historical_worst_pnl, current_return_pct,
        cluster_name, cluster_risk, setup_move_pct, max_pump_pct, live_snapshots,
        liquidation_price, reward_pct, risk_pct, reward_risk,
        virtual_position_usdt, virtual_balance_at_open,
    ) = row

    token_url = f'https://catapult.trade/ru/turbo/tokens/{token_id}'
    title = 'PAPER CALL' if virtual_position_usdt is not None else 'CALL'
    paper_line = ''
    if virtual_position_usdt is not None:
        paper_line = f'Paper: {fmt_money(virtual_position_usdt)} | Free: {fmt_money(virtual_balance_at_open)}\n'
    return (
        f'{title}\n'
        f'{token_label(token_name, token_symbol, token_id)}\n'
        f'{side} {mode}\n'
        f'Entry: {fmt_price(current_price)}\n'
        f'TP: {fmt_price(take_profit_price)}\n'
        f'SL: {fmt_price(stop_loss_price)}\n'
        f'{paper_line}'
        f'Link: {token_url}\n'
        f'ID: {signal_id}'
    )


def broadcast_new_signals():
    opened = open_missing_paper_trades()
    if opened:
        print(f'opened paper trades: {opened}', flush=True)
    fill_missing_virtual_open_fields()
    cur.execute('''
    SELECT
        ls.id, ls.token_id, os.name, os.symbol, ls.mode, ls.side, ls.confidence_pct,
        ls.current_price, ls.entry_low, ls.entry_high, ls.take_profit_price, ls.stop_loss_price,
        ls.max_leverage, ls.matched_strategy, ls.historical_trades, ls.historical_winrate,
        ls.historical_avg_pnl, ls.historical_median_pnl, ls.historical_worst_pnl, ls.current_return_pct,
        ls.cluster_name, ls.cluster_risk, ls.reversal_from_peak_pct, ls.max_pump_pct, ls.live_snapshots,
        ls.liquidation_price, ls.reward_pct, ls.risk_pct, ls.reward_risk,
        pst.virtual_position_usdt, pst.virtual_balance_at_open
    FROM live_signals ls
    LEFT JOIN paper_signal_trades pst ON pst.signal_id = ls.id
    LEFT JOIN official_api_token_state os ON os.token_id = ls.token_id
    WHERE COALESCE(ls.sent_to_telegram, false) = false
      AND COALESCE(ls.signal_status, 'OPEN') <> 'CANCELLED'
      AND COALESCE(ls.reason, '') NOT LIKE '%%CANCELLED_QUALITY_GATE%%'
      AND ls.created_at >= NOW() - (%s || ' minutes')::interval
    ORDER BY ls.confidence_pct DESC NULLS LAST,
             ls.historical_avg_pnl DESC NULLS LAST,
             ls.created_at ASC,
             ls.id ASC
    LIMIT 10
    ''', (MAX_SIGNAL_AGE_MINUTES,))

    rows = cur.fetchall()
    sent = 0
    for row in rows:
        signal_id = row[0]
        try:
            message_id = tg_send(signal_message(row))
            cur.execute(
                'UPDATE live_signals SET sent_to_telegram = true, telegram_message_id = %s WHERE id = %s',
                (message_id, signal_id),
            )
            sent += 1
            print(f'sent signal {signal_id}', flush=True)
        except Exception as exc:
            print(f'failed to send signal {signal_id}: {exc}', flush=True)
    return sent


def update_open_paper_trades():
    fill_missing_virtual_open_fields()
    cur.execute('''
    SELECT
        pst.id, pst.signal_id, pst.token_id, UPPER(pst.side),
        pst.entry_price, pst.take_profit_price, pst.stop_loss_price,
        pst.max_leverage, pst.virtual_position_usdt, pst.liquidation_price,
        COALESCE(pst.capital_reserved, false), ltf.current_price
    FROM paper_signal_trades pst
    LEFT JOIN live_token_features ltf ON ltf.token_id = pst.token_id
    WHERE pst.status = 'OPEN'
    ORDER BY pst.id ASC
    LIMIT 200
    ''')

    rows = cur.fetchall()
    closed = 0
    for row in rows:
        trade_id, signal_id, token_id, side, entry_price, tp, sl, max_leverage, position_usdt, liquidation_price, capital_reserved, current_price = row
        if current_price is None or entry_price is None or entry_price <= 0:
            continue

        close_reason = None
        if side == 'SHORT':
            if liquidation_price is not None and current_price >= liquidation_price:
                close_reason = 'LIQUIDATION'
            elif tp is not None and current_price <= tp:
                close_reason = 'TP'
            elif sl is not None and current_price >= sl:
                close_reason = 'SL'
        else:
            if liquidation_price is not None and current_price <= liquidation_price:
                close_reason = 'LIQUIDATION'
            elif tp is not None and current_price >= tp:
                close_reason = 'TP'
            elif sl is not None and current_price <= sl:
                close_reason = 'SL'

        if not close_reason:
            continue

        leveraged_pnl_pct = paper_pnl_pct(side, entry_price, current_price, max_leverage, close_reason)
        if leveraged_pnl_pct is None:
            continue
        position_usdt = float(position_usdt or effective_trade_size())
        virtual_pnl_usdt = position_usdt * leveraged_pnl_pct / 100.0
        balance_delta = virtual_pnl_usdt + (position_usdt if capital_reserved else 0.0)
        balance_after = update_account_balance(balance_delta)

        cur.execute('''
        UPDATE paper_signal_trades
        SET status = 'CLOSED',
            closed_at = %s,
            close_price = %s,
            pnl_pct = %s,
            close_reason = %s,
            virtual_position_usdt = %s,
            virtual_pnl_usdt = %s,
            virtual_balance_after_close = %s,
            capital_reserved = false
        WHERE id = %s
        ''', (
            utcnow(), current_price, leveraged_pnl_pct, close_reason,
            position_usdt, virtual_pnl_usdt, balance_after, trade_id,
        ))
        closed += 1
        print(
            f'closed paper trade {trade_id} signal={signal_id} {close_reason} '
            f'pnl={leveraged_pnl_pct:.2f}% virtual={virtual_pnl_usdt:.2f}',
            flush=True,
        )
    return closed


def send_closed_trade_results():
    cur.execute('''
    SELECT
        pst.id, pst.signal_id, pst.token_id, os.name, os.symbol, pst.mode, UPPER(pst.side), pst.confidence_pct,
        pst.entry_price, pst.close_price, pst.pnl_pct, pst.close_reason, pst.max_leverage,
        pst.opened_at, pst.closed_at, ls.cluster_name, ls.reversal_from_peak_pct,
        pst.liquidation_price, pst.reward_pct, pst.risk_pct, pst.reward_risk,
        pst.virtual_position_usdt, pst.virtual_pnl_usdt, pst.virtual_balance_after_close
    FROM paper_signal_trades pst
    LEFT JOIN live_signals ls ON ls.id = pst.signal_id
    LEFT JOIN official_api_token_state os ON os.token_id = pst.token_id
    WHERE pst.status = 'CLOSED'
      AND COALESCE(pst.result_sent_to_telegram, false) = false
    ORDER BY pst.closed_at ASC
    LIMIT 20
    ''')
    rows = cur.fetchall()
    sent = 0
    for row in rows:
        (
            trade_id, signal_id, token_id, token_name, token_symbol, mode, side, confidence_pct,
            entry_price, close_price, pnl_pct, close_reason, max_leverage,
            opened_at, closed_at, cluster_name, setup_move_pct,
            liquidation_price, reward_pct, risk_pct, reward_risk,
            virtual_position_usdt, virtual_pnl_usdt, balance_after,
        ) = row

        token_url = f'https://catapult.trade/ru/turbo/tokens/{token_id}'
        result_label = {
            'TP': 'TAKE PROFIT',
            'SL': 'STOP LOSS',
            'LIQUIDATION': 'LIQUIDATION',
        }.get(close_reason, close_reason or 'CLOSED')
        text = (
            f'PAPER CLOSED: {result_label}\n'
            f'{token_label(token_name, token_symbol, token_id)}\n'
            f'{side} {mode} | x{max_leverage:g}\n'
            f'Link: {token_url}\n\n'
            f'Entry: {fmt_price(entry_price)}\n'
            f'Close: {fmt_price(close_price)}\n'
            f'PnL: {pnl_pct:.2f}% ({fmt_money(virtual_pnl_usdt)})\n'
            f'Free: {fmt_money(balance_after)}\n'
            f'ID: {signal_id}'
        )
        try:
            tg_send(text)
            cur.execute('UPDATE paper_signal_trades SET result_sent_to_telegram = true WHERE id = %s', (trade_id,))
            sent += 1
        except Exception as exc:
            print(f'failed to send trade result {trade_id}: {exc}', flush=True)
    return sent


def main():
    ensure_schema()
    print('signal broadcaster started', flush=True)
    print(
        f'chat_id={SIGNAL_CHAT_ID} interval={INTERVAL_SECONDS}s '
        f'paper_start={PAPER_START_BALANCE_USDT} trade_size={configured_trade_size()} '
        f'auto_open={paper_auto_open_enabled()} max_open={max_open_trades()}',
        flush=True,
    )
    while True:
        try:
            sent = broadcast_new_signals()
            closed = update_open_paper_trades()
            result_sent = send_closed_trade_results()
            if sent or closed or result_sent:
                print(f'cycle sent={sent} closed={closed} result_sent={result_sent}', flush=True)
        except Exception as exc:
            print(f'signal broadcaster error: {exc}', flush=True)
        time.sleep(INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
