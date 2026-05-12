import os
import subprocess
import sys
import time

processes = []

if not os.getenv('DATABASE_URL'):
    print('ERROR: DATABASE_URL is missing. Add Railway Postgres and reference Postgres.DATABASE_URL.', flush=True)
    sys.exit(1)

bot_only = os.getenv('BOT_ONLY', '').lower() in ('1', 'true', 'yes', 'on')

if bot_only:
    print('BOT_ONLY=true. Collector disabled on Railway.', flush=True)
else:
    print('Starting direct GraphQL collector...', flush=True)
    processes.append(subprocess.Popen([sys.executable, 'direct_collector.py']))

if os.getenv('BOT_TOKEN'):
    print('BOT_TOKEN found. Starting Telegram bot...', flush=True)
    processes.append(subprocess.Popen([sys.executable, 'telegram_bot.py']))
else:
    print('BOT_TOKEN not set. Telegram bot disabled.', flush=True)

if not processes:
    print('No processes started. Set BOT_TOKEN and/or disable BOT_ONLY.', flush=True)
    sys.exit(1)

try:
    while True:
        for proc in processes:
            code = proc.poll()
            if code is not None:
                print(f'Child process exited with code {code}. Stopping app.', flush=True)
                sys.exit(code)
        time.sleep(5)
except KeyboardInterrupt:
    for proc in processes:
        proc.terminate()
