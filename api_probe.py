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

HEADER_CANDIDATES = [
    ('Authorization', f'Bearer {API_KEY}'),
    ('x-api-key', API_KEY),
    ('X-API-Key', API_KEY),
]

INTROSPECTION_QUERY = '''
query ApiProbeQueryFields {
  __schema {
    queryType {
      fields {
        name
        description
        args {
          name
          type {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
    }
  }
}
'''

TYPE_QUERY = '''
query ApiProbeTypes {
  __schema {
    types {
      name
      kind
      fields {
        name
      }
    }
  }
}
'''


def post_graphql(headers, query, variables=None, operation_name=None):
    payload = {'query': query}
    if variables is not None:
        payload['variables'] = variables
    if operation_name is not None:
        payload['operationName'] = operation_name
    return requests.post(API_URL, headers=headers, json=payload, timeout=30)


def compact_type(type_obj):
    if not type_obj:
        return 'unknown'
    kind = type_obj.get('kind')
    name = type_obj.get('name')
    inner = type_obj.get('ofType')
    if kind == 'NON_NULL':
        return compact_type(inner) + '!'
    if kind == 'LIST':
        return '[' + compact_type(inner) + ']'
    return name or kind or 'unknown'


def print_query_fields(data):
    fields = data.get('data', {}).get('__schema', {}).get('queryType', {}).get('fields') or []
    if not fields:
        print('No query fields returned by introspection.')
        return []

    print('\nAvailable Query fields:')
    for field in fields:
        args = field.get('args') or []
        arg_text = ', '.join(f"{a.get('name')}: {compact_type(a.get('type'))}" for a in args)
        if arg_text:
            print(f"- {field.get('name')}({arg_text})")
        else:
            print(f"- {field.get('name')}")
    return fields


def print_token_related_types(data):
    types = data.get('data', {}).get('__schema', {}).get('types') or []
    matches = []
    for type_item in types:
        name = type_item.get('name') or ''
        if any(word in name.lower() for word in ('token', 'turbo', 'price', 'stream')):
            field_names = [f.get('name') for f in (type_item.get('fields') or []) if f.get('name')]
            matches.append((name, type_item.get('kind'), field_names[:20]))

    print('\nToken/turbo/price related schema types:')
    if not matches:
        print('- none found')
        return

    for name, kind, field_names in matches[:80]:
        suffix = ''
        if field_names:
            suffix = ' fields=' + ', '.join(field_names)
        print(f'- {name} [{kind}]{suffix}')


print(f'API_URL={API_URL}')
print(f'TOKEN_ID={TOKEN_ID}')
print('This probe is read-only. It only uses GraphQL introspection and does not call trading mutations.')

for i, (header_name, header_value) in enumerate(HEADER_CANDIDATES, start=1):
    headers = {
        'accept': 'application/json',
        'content-type': 'application/json',
        header_name: header_value,
    }

    print('\n' + '=' * 80)
    print(f'Test #{i}: {header_name}')

    try:
        response = post_graphql(headers, INTROSPECTION_QUERY, operation_name='ApiProbeQueryFields')
    except Exception as exc:
        print(f'request error: {exc}')
        continue

    print(f'status={response.status_code}')
    print(response.text[:1200])

    try:
        data = response.json()
    except Exception:
        print('Response is not JSON.')
        continue

    if response.status_code == 200 and data and not data.get('errors'):
        print('\nSUCCESS: this auth header can access schema introspection')
        print_query_fields(data)

        try:
            type_response = post_graphql(headers, TYPE_QUERY, operation_name='ApiProbeTypes')
            type_data = type_response.json()
            if type_response.status_code == 200 and not type_data.get('errors'):
                print_token_related_types(type_data)
        except Exception as exc:
            print(f'type introspection skipped: {exc}')
        break

    errors = data.get('errors') if isinstance(data, dict) else None
    if errors:
        print('\nerrors preview:')
        print(json.dumps(errors, ensure_ascii=False, indent=2)[:1600])
else:
    print('\nNo header candidate succeeded. Check API key scopes and endpoint.')
