import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv('CATAPULT_API_URL', 'https://public-api.catapult.trade/graphql').strip()
API_KEY = os.getenv('CATAPULT_API_KEY', '').strip()
TOKEN_ID = os.getenv('PROBE_TOKEN_ID', '15836020').strip()

if not API_KEY:
    print('CATAPULT_API_KEY is missing in .env')
    print('Create a new read-only API key with only Read Tokens / Stream Price scopes.')
    sys.exit(1)

QUERY = '''
query TurboTokenDetailsV2($tokenId: String!) {
  turboTokenDetailsV2(tokenId: $tokenId) {
    id
    name
    symbol
    speedMode
    initialPrice
    startDate
    endDate
    isExpired
    __typename
  }
}
'''

payload = {
    'query': QUERY,
    'variables': {'tokenId': str(TOKEN_ID)},
    'operationName': 'TurboTokenDetailsV2',
}

header_candidates = [
    ('Authorization', f'Bearer {API_KEY}'),
    ('x-api-key', API_KEY),
    ('X-API-Key', API_KEY),
]

print(f'API_URL={API_URL}')
print(f'TOKEN_ID={TOKEN_ID}')
print('This probe is read-only and only queries token details.')

for i, (header_name, header_value) in enumerate(header_candidates, start=1):
    headers = {
        'accept': 'application/json',
        'content-type': 'application/json',
        header_name: header_value,
    }

    print('\n' + '=' * 80)
    print(f'Test #{i}: {header_name}')

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
    except Exception as exc:
        print(f'request error: {exc}')
        continue

    print(f'status={response.status_code}')
    print(response.text[:2000])

    try:
        data = response.json()
    except Exception:
        data = None

    if data is not None:
        print('\nparsed json preview:')
        print(json.dumps(data, ensure_ascii=False, indent=2)[:2000])

    if response.status_code == 200 and data and not data.get('errors'):
        print('\nSUCCESS: this auth header seems to work')
        break
else:
    print('\nNo header candidate succeeded. Check API key scopes, endpoint, and token id.')
