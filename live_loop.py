import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

INTERVAL_SECONDS = float(os.getenv('LIVE_LOOP_INTERVAL_SECONDS', '20'))
RUN_FEATURES = os.getenv('LIVE_LOOP_RUN_FEATURES', 'true').lower() in ('1', 'true', 'yes', 'on')
RUN_SIGNALS = os.getenv('LIVE_LOOP_RUN_SIGNALS', 'true').lower() in ('1', 'true', 'yes', 'on')


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def run_script(script):
    print(f'\n{utcnow()} UTC | START | {script}', flush=True)
    proc = subprocess.run(
        [sys.executable, script],
        text=True,
        capture_output=True,
    )

    if proc.stdout:
        print(proc.stdout.strip(), flush=True)
    if proc.stderr:
        print(proc.stderr.strip(), flush=True)

    if proc.returncode != 0:
        print(f'{utcnow()} UTC | ERROR | {script} | code={proc.returncode}', flush=True)
        return False

    print(f'{utcnow()} UTC | OK | {script}', flush=True)
    return True


def main():
    print('live loop started', flush=True)
    print(f'interval={INTERVAL_SECONDS}s run_features={RUN_FEATURES} run_signals={RUN_SIGNALS}', flush=True)
    print('Keep official_api_collector.py running in another terminal/service.', flush=True)
    print('Keep signal_broadcaster.py running in another terminal/service for Telegram + paper trade tracking.', flush=True)

    while True:
        cycle_started = utcnow()
        print(f'\n================================================================================', flush=True)
        print(f'{cycle_started} UTC | LIVE CYCLE START', flush=True)
        print(f'================================================================================', flush=True)

        if RUN_FEATURES:
            run_script('live_features.py')

        if RUN_SIGNALS:
            run_script('live_signals.py')

        print(f'{utcnow()} UTC | LIVE CYCLE DONE | sleeping {INTERVAL_SECONDS}s', flush=True)
        time.sleep(INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
