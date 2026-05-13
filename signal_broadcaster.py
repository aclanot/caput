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


def ensure_schema():
    cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS sent_to_telegram BOOLEAN DEFAULT FALSE')
    cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT')
    cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_name TEXT')
    cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS cluster_risk TEXT')
    cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS reversal_from_peak_pct DOUBLE PRECISION')
    cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS max_pump_pct DOUBLE PRECISION')
    cur.execute('ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS live_snapshots INT')
    cur.execute('ALTER TABLE paper_signal_trades ADD COLUMN IF NOT EXISTS result_sent_to_telegram BOOLEAN DEFAULT FALSE')


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
    if abs(value) >= 100:
        return f'{value:.2f}'
    if abs(value) >= 1:
        return f'{value:.4f}'
    return f'{value:.8f}'


def signal_message(row):
    (
        signal_id, token_id, mode, side, confidence_pct,
        current_price, entry_low, entry_high, take_profit_price, stop_loss_price,
        max_leverage, matched_strategy, historical_trades, historical_winrate,
        historical_avg_pnl, historical_median_pnl, historical_worst_pnl, current_return_pct,
        cluster_name, cluster_risk, reversal_from_peak_pct, max_pump_pct, live_snapshots,
    ) = row

    token_url = f'https://catapult.trade/ru/turbo/tokens/{token_id}'

    return (
        '🚨 PAPER SIGNAL\n\n'
        f'Token: {token_url}\n'
        f'Type: {side}\n'
        f'Mode: {mode}\n'
        f'Confidence: {confidence_pct}%\n'
        f'Max leverage: x{max_leverage:g}\n\n'
        f'Cluster: {cluster_name or "n/a"}\n'
        f'Cluster risk: {cluster_risk or "n/a"}\n'
        f'Snapshots: {live_snapshots or 0}\n'
        f'Max pump: {(max_pump_pct or 0):.2f}%\n'
        f'Reversal from peak: {(reversal_from_peak_pct or 0):.2f}%\n\n'
        f'Current price: {fmt_price(current_price)}\n'
        f'Entry: {fmt_price(entry_low)} - {fmt_price(entry_high)}\n'
        f'Take profit: {fmt_price(take_profit_price)}\n'
        f'Stop loss: {fmt_price(stop_loss_price)}\n\n'
        f'Current return: {current_return_pct:.2f}%\n'
        f'Strategy: {matched_strategy}\n\n'
        'Historical stats:\n'
        f'Trades: {historical_trades}\n'
        f'Winrate: {historical_winrate * 100:.1f}%\n'
        f'Avg pnl: {historical_avg_pnl:.2f}%\n'
        f'Median pnl: {historical_median_pnl:.2f}%\n'
        f'Worst pnl: {historical_worst_pnl:.2f}%\n\n'
        f'Paper trade opened. Signal ID: {signal_id}'
    )


def broadcast_new_signals():
    cur.execute('''
    SELECT
        id, token_id, mode, side, confidence_pct,
        current_price, entry_low, entry_high, take_profit_price, stop_loss_price,
        max_leverage, matched_strategy, historical_trades, historical_winrate,
        historical_avg_pnl, historical_median_pnl, historical_worst_pnl, current_return_pct,
        cluster_name, cluster_risk, reversal_from_peak_pct, max_pump_pct, live_snapshots
    FROM live_signals
    WHERE COALESCE(sent_to_telegram, false) = false
      AND created_at >= NOW() - (%s || ' minutes')::interval
    ORDER BY confidence_pct DESC NULLS LAST, id ASC
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
    cur.execute('''
    SELECT
        pst.id, pst.signal_id, pst.token_id, pst.side,
        pst.entry_price, pst.take_profit_price, pst.stop_loss_price,
        pst.max_leverage, pst.opened_at, ltf.current_price
    FROM paper_signal_trades pst
    LEFT JOIN live_token_features ltf ON ltf.token_id = pst.token_id
    WHERE pst.status = 'OPEN'
    ORDER BY pst.id ASC
    LIMIT 200
    ''')

    rows = cur.fetchall()
    closed = 0
    for row in rows:
        trade_id, signal_id, token_id, side, entry_price, tp, sl, max_leverage, opened_at, current_price = row
        if current_price is None or entry_price is None or entry_price <= 0:
            continue

        close_reason = None
        if side == 'SHORT':
            if tp is not None and current_price <= tp:
                close_reason = 'TP'
            elif sl is not None and current_price >= sl:
                close_reason = 'SL'
            pnl_pct = (entry_price / current_price - 1.0) * 100.0
        else:
            if tp is not None and current_price >= tp:
                close_reason = 'TP'
            elif sl is not None and current_price <= sl:
                close_reason = 'SL'
            pnl_pct = (current_price / entry_price - 1.0) * 100.0

        if not close_reason:
            continue

        leveraged_pnl = pnl_pct * (max_leverage or 1.0)
        cur.execute('''
        UPDATE paper_signal_trades
        SET status = 'CLOSED', closed_at = %s, close_price = %s, pnl_pct = %s, close_reason = %s
        WHERE id = %s
        ''', (utcnow(), current_price, leveraged_pnl, close_reason, trade_id))
        closed += 1
        print(f'closed paper trade {trade_id} {close_reason} pnl={leveraged_pnl:.2f}%', flush=True)
    return closed


def send_closed_trade_results():
    cur.execute('''
    SELECT
        pst.id, pst.signal_id, pst.token_id, pst.mode, pst.side, pst.confidence_pct,
        pst.entry_price, pst.close_price, pst.pnl_pct, pst.close_reason, pst.max_leverage,
        pst.opened_at, pst.closed_at, ls.cluster_name, ls.reversal_from_peak_pct
    FROM paper_signal_trades pst
    LEFT JOIN live_signals ls ON ls.id = pst.signal_id
    WHERE pst.status = 'CLOSED'
      AND COALESCE(pst.result_sent_to_telegram, false) = false
    ORDER BY pst.closed_at ASC
    LIMIT 20
    ''')
    rows = cur.fetchall()
    sent = 0
    for row in rows:
        (
            trade_id, signal_id, token_id, mode, side, confidence_pct,
            entry_price, close_price, pnl_pct, close_reason, max_leverage,
            opened_at, closed_at, cluster_name, reversal_from_peak_pct,
        ) = row

        token_url = f'https://catapult.trade/ru/turbo/tokens/{token_id}'
        icon = '✅' if close_reason == 'TP' else '❌'
        text = (
            f'{icon} PAPER TRADE CLOSED\n\n'
            f'Token: {token_url}\n'
            f'Signal ID: {signal_id}\n'
            f'Type: {side}\n'
            f'Mode: {mode}\n'
            f'Confidence: {confidence_pct}%\n'
            f'Cluster: {cluster_name or "n/a"}\n'
            f'Reversal from peak: {(reversal_from_peak_pct or 0):.2f}%\n'
            f'Leverage: x{max_leverage:g}\n\n'
            f'Entry: {fmt_price(entry_price)}\n'
            f'Close: {fmt_price(close_price)}\n'
            f'Reason: {close_reason}\n'
            f'Paper PnL: {pnl_pct:.2f}%\n\n'
            f'Opened: {opened_at}\n'
            f'Closed: {closed_at}'
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
    print(f'chat_id={SIGNAL_CHAT_ID} interval={INTERVAL_SECONDS}s', flush=True)
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
