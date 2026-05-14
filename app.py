import os
import signal
import subprocess
import sys
import time

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv:
    load_dotenv()

RESTART_INITIAL_SECONDS = float(os.getenv('WORKER_RESTART_INITIAL_SECONDS', '5'))
RESTART_MAX_SECONDS = float(os.getenv('WORKER_RESTART_MAX_SECONDS', '60'))
STABLE_RUN_SECONDS = float(os.getenv('WORKER_STABLE_RUN_SECONDS', '120'))
SUPERVISOR_SLEEP_SECONDS = float(os.getenv('APP_SUPERVISOR_SLEEP_SECONDS', '5'))


def truthy(value):
    return str(value or '').lower() in ('1', 'true', 'yes', 'on')


def enabled(name, default=True):
    value = os.getenv(name)
    if value is None:
        return default
    return truthy(value)


def make_worker(label, script, required_env=()):
    return {
        'label': label,
        'script': script,
        'required_env': tuple(required_env),
        'proc': None,
        'started_at': None,
        'restart_count': 0,
        'next_start_at': 0.0,
    }


def prepare_workers(startup_plan):
    workers = []
    for label, script, should_run, required_env in startup_plan:
        if not should_run:
            print(f'{label} disabled by env.', flush=True)
            continue

        missing = [name for name in required_env if not os.getenv(name)]
        if missing:
            print(f'{label} disabled. Missing env: {", ".join(missing)}', flush=True)
            continue

        workers.append(make_worker(label, script, required_env=required_env))
    return workers


def schedule_restart(worker, reason):
    worker['restart_count'] += 1
    exponent = min(worker['restart_count'] - 1, 5)
    delay = min(RESTART_MAX_SECONDS, RESTART_INITIAL_SECONDS * (2 ** exponent))
    worker['next_start_at'] = time.monotonic() + delay
    print(f'{worker["label"]} restart scheduled in {delay:g}s ({reason}).', flush=True)


def start_worker(worker):
    label = worker['label']
    script = worker['script']
    print(f'Starting {label}: {script}', flush=True)
    try:
        worker['proc'] = subprocess.Popen([sys.executable, '-u', script])
        worker['started_at'] = time.monotonic()
        worker['next_start_at'] = 0.0
        return True
    except Exception as exc:
        worker['proc'] = None
        worker['started_at'] = None
        print(f'{label} failed to start: {exc}', flush=True)
        schedule_restart(worker, 'start failed')
        return False


def stop_processes(workers):
    for worker in workers:
        label = worker['label']
        proc = worker['proc']
        if proc is None:
            continue
        if proc.poll() is None:
            print(f'Stopping {label}', flush=True)
            proc.terminate()
    for worker in workers:
        label = worker['label']
        proc = worker['proc']
        if proc is None:
            continue
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f'Killing {label}', flush=True)
            proc.kill()


if not os.getenv('DATABASE_URL'):
    print('ERROR: DATABASE_URL is missing.', flush=True)
    sys.exit(1)

workers = []
bot_only = truthy(os.getenv('BOT_ONLY'))

if bot_only:
    workers = prepare_workers([
        ('telegram bot', 'telegram_bot.py', True, ('BOT_TOKEN',)),
    ])
else:
    startup_plan = [
        (
            'official API collector',
            'official_api_collector.py',
            enabled('RUN_OFFICIAL_COLLECTOR', default=bool(os.getenv('CATAPULT_API_KEY'))),
            ('CATAPULT_API_KEY',),
        ),
        (
            'live loop',
            'live_loop.py',
            enabled('RUN_LIVE_LOOP', default=True),
            (),
        ),
        (
            'signal broadcaster',
            'signal_broadcaster.py',
            enabled('RUN_SIGNAL_BROADCASTER', default=bool(os.getenv('BOT_TOKEN') and os.getenv('TELEGRAM_SIGNAL_CHAT_ID'))),
            ('BOT_TOKEN', 'TELEGRAM_SIGNAL_CHAT_ID'),
        ),
        (
            'telegram bot',
            'telegram_bot.py',
            enabled('RUN_TELEGRAM_BOT', default=bool(os.getenv('BOT_TOKEN'))),
            ('BOT_TOKEN',),
        ),
    ]

    workers = prepare_workers(startup_plan)

if not workers:
    print('No processes started. Check RUN_* flags and required environment variables.', flush=True)
    sys.exit(1)

for worker in workers:
    start_worker(worker)


def handle_shutdown(signum, frame):
    print(f'Received signal {signum}. Shutting down.', flush=True)
    stop_processes(workers)
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

while True:
    now = time.monotonic()
    for worker in workers:
        proc = worker['proc']
        if proc is None:
            if now >= worker['next_start_at']:
                start_worker(worker)
            continue

        code = proc.poll()
        if code is not None:
            label = worker['label']
            started_at = worker['started_at'] or now
            runtime = now - started_at
            print(f'{label} exited with code {code} after {runtime:.1f}s.', flush=True)
            worker['proc'] = None
            worker['started_at'] = None
            if runtime >= STABLE_RUN_SECONDS:
                worker['restart_count'] = 0
            schedule_restart(worker, f'exit code {code}')
    time.sleep(SUPERVISOR_SLEEP_SECONDS)
