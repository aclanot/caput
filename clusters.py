import os
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS trajectory_clusters (
    token_id TEXT PRIMARY KEY,
    mode TEXT,
    cluster_name TEXT,
    cluster_reason TEXT,
    final_return_pct DOUBLE PRECISION,
    max_pump_pct DOUBLE PRECISION,
    max_drawdown_pct DOUBLE PRECISION,
    updated_at TIMESTAMP
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS cluster_summary (
    cluster_name TEXT,
    mode TEXT,
    tokens INT,
    avg_final_return_pct DOUBLE PRECISION,
    avg_max_pump_pct DOUBLE PRECISION,
    avg_max_drawdown_pct DOUBLE PRECISION,
    p_final_below_start DOUBLE PRECISION,
    p_pump30_dump20 DOUBLE PRECISION,
    updated_at TIMESTAMP,
    PRIMARY KEY(cluster_name, mode)
)
''')

cur.execute('DELETE FROM cluster_summary')


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def classify_cluster(final_ret, max_pump, max_dd, pump_fail, bounce):
    if max_pump >= 500 and final_ret >= 100:
        return 'mega_pump', 'huge pump and strong close'
    if max_pump >= 200 and final_ret < 0:
        return 'pump_then_rug', 'large pump but closed below start'
    if max_pump >= 50 and pump_fail:
        return 'fake_pump_failure', 'pump then failed below start zone'
    if max_dd <= -90 and bounce:
        return 'dead_cat_bounce', 'extreme drawdown with rebound'
    if max_dd <= -90:
        return 'dead_no_bounce', 'extreme drawdown with no recovery'
    if final_ret < 0 and max_pump < 50:
        return 'slow_bleed', 'weak trajectory with negative close'
    if final_ret >= 100:
        return 'strong_finish', 'strong positive close'
    return 'mixed_noise', 'unclassified trajectory'


cur.execute('''
SELECT token_id, COALESCE(mode, 'UNKNOWN'), final_return_pct,
       max_pump_pct, max_drawdown_pct,
       COALESCE(pump30_dump20, false),
       COALESCE(dead_bounce_20, false)
FROM trajectory_features
''')
rows = cur.fetchall()

summary = defaultdict(list)
processed = 0
now = utcnow()

for row in rows:
    token_id, mode, final_ret, max_pump, max_dd, pump_fail, bounce = row
    cluster_name, reason = classify_cluster(final_ret, max_pump, max_dd, pump_fail, bounce)
    cur.execute('''
    INSERT INTO trajectory_clusters(
        token_id, mode, cluster_name, cluster_reason,
        final_return_pct, max_pump_pct, max_drawdown_pct, updated_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(token_id) DO UPDATE SET
        mode = EXCLUDED.mode,
        cluster_name = EXCLUDED.cluster_name,
        cluster_reason = EXCLUDED.cluster_reason,
        final_return_pct = EXCLUDED.final_return_pct,
        max_pump_pct = EXCLUDED.max_pump_pct,
        max_drawdown_pct = EXCLUDED.max_drawdown_pct,
        updated_at = EXCLUDED.updated_at
    ''', (str(token_id), mode, cluster_name, reason, final_ret, max_pump, max_dd, now))
    summary[(cluster_name, mode)].append((final_ret, max_pump, max_dd, pump_fail))
    processed += 1

for (cluster_name, mode), values in summary.items():
    n = len(values)
    avg_final = sum(v[0] for v in values) / n
    avg_pump = sum(v[1] for v in values) / n
    avg_dd = sum(v[2] for v in values) / n
    p_down = sum(1 for v in values if v[0] < 0) / n
    p_pump_fail = sum(1 for v in values if v[3]) / n
    cur.execute('''
    INSERT INTO cluster_summary(
        cluster_name, mode, tokens, avg_final_return_pct, avg_max_pump_pct,
        avg_max_drawdown_pct, p_final_below_start, p_pump30_dump20, updated_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(cluster_name, mode) DO UPDATE SET
        tokens = EXCLUDED.tokens,
        avg_final_return_pct = EXCLUDED.avg_final_return_pct,
        avg_max_pump_pct = EXCLUDED.avg_max_pump_pct,
        avg_max_drawdown_pct = EXCLUDED.avg_max_drawdown_pct,
        p_final_below_start = EXCLUDED.p_final_below_start,
        p_pump30_dump20 = EXCLUDED.p_pump30_dump20,
        updated_at = EXCLUDED.updated_at
    ''', (cluster_name, mode, n, avg_final, avg_pump, avg_dd, p_down, p_pump_fail, now))

print(f'clustered trajectories: {processed}')
print(f'cluster groups prepared: {len(summary)}')
print(f'clusters completed at {utcnow()} UTC')
