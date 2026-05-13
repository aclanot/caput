import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

INTERVAL_SECONDS = float(os.getenv('LIVE_LOOP_INTERVAL_SECONDS', '5'))
RUN_PERFORMANCE = os.getenv('LIVE_LOOP_RUN_PERFORMANCE', 'true').lower() in ('1', 'true', 'yes', 'on')
PERFORMANCE_EVERY_N = int(os.getenv('LIVE_LOOP_PERFORMANCE_EVERY_N', '12'))


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def run_script(script):
    print(f'\n{utcnow()} UTC | START | {script}', flush=True)
    proc = subprocess.run([sys.executable, script], text=True, capture_output=True)
    if proc.stdout:
        print(proc.stdout.strip(), flush=True)
    if proc.stderr:
        print(proc.stderr.strip(), flush=True)
    if proc.returncode != 0:
        print(f'{utcnow()} UTC | ERROR | {script} | code={proc.returncode}', flush=True)
    else:
        print(f'{utcnow()} UTC | OK | {script}', flush=True)


def main():
    print('live loop started', flush=True)
    print(f'interval={INTERVAL_SECONDS}s run_performance={RUN_PERFORMANCE} performance_every_n={PERFORMANCE_EVERY_N}', flush=True)
    cycle = 0
    while True:
        cycle += 1
        print(f'\n{utcnow()} UTC | CYCLE {cycle} START', flush=True)
        run_script('live_features.py')
        run_script('live_signals.py')
        if RUN_PERFORMANCE and PERFORMANCE_EVERY_N > 0 and cycle % PERFORMANCE_EVERY_N == 0:
            run_script('live_performance.py')
        print(f'{utcnow()} UTC | CYCLE {cycle} DONE | sleep={INTERVAL_SECONDS}s', flush=True)
        time.sleep(INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
