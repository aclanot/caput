import os
import signal
import subprocess
import sys
import time


def truthy(value):
    return str(value or '').lower() in ('1', 'true', 'yes', 'on')


def enabled(name, default=True):
    value = os.getenv(name)
    if value is None:
        return default
    return truthy(value)


def start_process(label, script, required_env=()):
    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        print(f'{label} disabled. Missing env: {", ".join(missing)}', flush=True)
        return None

    print(f'Starting {label}: {script}', flush=True)
    return label, subprocess.Popen([sys.executable, '-u', script])


def stop_processes(processes):
    for label, proc in processes:
        if proc.poll() is None:
            print(f'Stopping {label}', flush=True)
            proc.terminate()
    for label, proc in processes:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f'Killing {label}', flush=True)
            proc.kill()


if not os.getenv('DATABASE_URL'):
    print('ERROR: DATABASE_URL is missing.', flush=True)
    sys.exit(1)

processes = []
bot_only = truthy(os.getenv('BOT_ONLY'))

if bot_only:
    maybe_proc = start_process('telegram bot', 'telegram_bot.py', required_env=('BOT_TOKEN',))
    if maybe_proc:
        processes.append(maybe_proc)
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

    for label, script, should_run, required_env in startup_plan:
        if not should_run:
            print(f'{label} disabled by env.', flush=True)
            continue
        maybe_proc = start_process(label, script, required_env=required_env)
        if maybe_proc:
            processes.append(maybe_proc)

if not processes:
    print('No processes started. Check RUN_* flags and required environment variables.', flush=True)
    sys.exit(1)


def handle_shutdown(signum, frame):
    print(f'Received signal {signum}. Shutting down.', flush=True)
    stop_processes(processes)
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

while True:
    for label, proc in processes:
        code = proc.poll()
        if code is not None:
            print(f'{label} exited with code {code}. Stopping app.', flush=True)
            stop_processes(processes)
            sys.exit(code)
    time.sleep(5)
