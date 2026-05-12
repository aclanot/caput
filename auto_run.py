import subprocess
import sys
from datetime import datetime

STEPS = [
    ('analyze.py', 'Build trajectory features'),
    ('paper_sim.py', 'Run paper simulation'),
    ('sweep.py', 'Run parameter sweep'),
]


def run_step(script, label):
    print('\n' + '=' * 80, flush=True)
    print(f'{datetime.utcnow()} UTC | START | {label} | {script}', flush=True)
    print('=' * 80, flush=True)

    result = subprocess.run([sys.executable, script], text=True)

    print('\n' + '=' * 80, flush=True)
    if result.returncode == 0:
        print(f'{datetime.utcnow()} UTC | OK | {label}', flush=True)
    else:
        print(f'{datetime.utcnow()} UTC | FAILED | {label} | exit={result.returncode}', flush=True)
        raise SystemExit(result.returncode)
    print('=' * 80, flush=True)


print(f'auto_run started at {datetime.utcnow()} UTC', flush=True)

for script, label in STEPS:
    run_step(script, label)

print(f'auto_run finished at {datetime.utcnow()} UTC', flush=True)
print('Now check Telegram: /status /summary /paper /sweep 20', flush=True)
