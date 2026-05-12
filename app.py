import os
import subprocess

service = os.getenv('SERVICE', 'collector')

if service == 'bot':
    subprocess.run(['python', 'telegram_bot.py'])
else:
    subprocess.run(['python', 'collector.py'])
