import os
from datetime import UTC, datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
READY_DELAY_SECONDS = int(os.getenv('FAST_FINISHED_READY_DELAY_SECONDS', '45'))

if not DATABASE_URL:
    raise SystemExit('DATABASE_URL is missing')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()


def table_exists(name):
    cur.execute('SELECT to_regclass(%s)', (f'public.{name}',))
    return cur.fetchone()[0] is not None


def one(sql, args=()):
    cur.execute(sql, args)
    row = cur.fetchone()
    return row[0] if row else None


def print_kv(label, value):
    print(f'{label}: {value}', flush=True)


print('DATA HEALTH')
print(f'checked_at_utc: {datetime.now(UTC).replace(tzinfo=None)}')
print(f'ready_delay_seconds: {READY_DELAY_SECONDS}')
print()

finished = one('SELECT COUNT(*) FROM finished_tokens') if table_exists('finished_tokens') else 0
snapshots = one('SELECT COUNT(*) FROM token_snapshots') if table_exists('token_snapshots') else 0
unique_snap_tokens = one('SELECT COUNT(DISTINCT token_id) FROM token_snapshots') if table_exists('token_snapshots') else 0
last_snapshot = one('SELECT MAX(ts) FROM token_snapshots') if table_exists('token_snapshots') else None

print_kv('finished_trajectories', finished)
print_kv('live_snapshots_rows', snapshots)
print_kv('unique_tokens_in_snapshots', unique_snap_tokens)
print_kv('last_snapshot', last_snapshot)
if unique_snap_tokens:
    print_kv('finished_vs_unique_snapshot_tokens_pct', round(finished / unique_snap_tokens * 100, 2))
print()

if table_exists('official_api_token_state'):
    state_count = one('SELECT COUNT(*) FROM official_api_token_state')
    state_ready = one(
        '''
        SELECT COUNT(*)
        FROM official_api_token_state s
        WHERE s.end_date IS NOT NULL
          AND s.end_date <= NOW() - (%s || ' seconds')::interval
        ''',
        (READY_DELAY_SECONDS,),
    )
    state_ready_unfinished = one(
        '''
        SELECT COUNT(*)
        FROM official_api_token_state s
        WHERE s.end_date IS NOT NULL
          AND s.end_date <= NOW() - (%s || ' seconds')::interval
          AND NOT EXISTS (SELECT 1 FROM finished_tokens ft WHERE ft.token_id = s.token_id)
        ''',
        (READY_DELAY_SECONDS,),
    )
    state_live_or_not_ready = one(
        '''
        SELECT COUNT(*)
        FROM official_api_token_state s
        WHERE s.end_date IS NULL
           OR s.end_date > NOW() - (%s || ' seconds')::interval
        ''',
        (READY_DELAY_SECONDS,),
    )
    print_kv('official_state_tokens', state_count)
    print_kv('official_state_ready_expired', state_ready)
    print_kv('official_state_ready_unfinished_queue', state_ready_unfinished)
    print_kv('official_state_live_or_waiting_delay', state_live_or_not_ready)
    if state_ready:
        print_kv('finished_vs_ready_expired_pct', round(finished / state_ready * 100, 2))
    print()

    cur.execute('''
    SELECT COALESCE(mode, 'UNKNOWN') AS mode, COUNT(*)
    FROM official_api_token_state
    GROUP BY COALESCE(mode, 'UNKNOWN')
    ORDER BY COUNT(*) DESC
    ''')
    print('official_state_by_mode')
    for mode, n in cur.fetchall():
        print(f'- {mode}: {n}')
    print()

    cur.execute(
        '''
        SELECT COALESCE(s.mode, 'UNKNOWN') AS mode, COUNT(*)
        FROM official_api_token_state s
        WHERE s.end_date IS NOT NULL
          AND s.end_date <= NOW() - (%s || ' seconds')::interval
          AND NOT EXISTS (SELECT 1 FROM finished_tokens ft WHERE ft.token_id = s.token_id)
        GROUP BY COALESCE(s.mode, 'UNKNOWN')
        ORDER BY COUNT(*) DESC
        ''',
        (READY_DELAY_SECONDS,),
    )
    print('ready_unfinished_by_mode')
    for mode, n in cur.fetchall():
        print(f'- {mode}: {n}')
    print()
else:
    print('official_api_token_state: missing. Run/pull latest official_api_collector.py first.')
    print()

if table_exists('fast_finished_scan_log'):
    cur.execute('''
    SELECT status, COUNT(*)
    FROM fast_finished_scan_log
    GROUP BY status
    ORDER BY COUNT(*) DESC
    LIMIT 20
    ''')
    print('fast_finished_scan_log_by_status')
    for status, n in cur.fetchall():
        print(f'- {status}: {n}')
    print()

    cur.execute('''
    SELECT status, COUNT(*)
    FROM fast_finished_scan_log
    WHERE ts >= NOW() - interval '30 minutes'
    GROUP BY status
    ORDER BY COUNT(*) DESC
    LIMIT 20
    ''')
    print('fast_finished_last_30m_by_status')
    for status, n in cur.fetchall():
        print(f'- {status}: {n}')
    print()

    cur.execute('''
    WITH last_status AS (
        SELECT DISTINCT ON (token_id)
            token_id, status, ts, note
        FROM fast_finished_scan_log
        ORDER BY token_id, ts DESC
    )
    SELECT status, COUNT(*)
    FROM last_status
    GROUP BY status
    ORDER BY COUNT(*) DESC
    LIMIT 20
    ''')
    print('latest_status_per_token')
    for status, n in cur.fetchall():
        print(f'- {status}: {n}')
    print()

print('recommended_reading')
print('- live_snapshots_rows is not token count; use unique_tokens_in_snapshots')
print('- if ready_unfinished_queue is high, keep fast_finished_collector running')
print('- if fast_finished_last_30m has mostly not_ready, increase FAST_FINISHED_READY_DELAY_SECONDS to 90 or 120')
print('- if official_state_tokens is much lower than unique snapshots, restart official_api_collector after git pull')
