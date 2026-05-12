import os
from datetime import datetime

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

print('cluster tables ready')
print(f'clusters bootstrap completed at {datetime.utcnow()} UTC')
