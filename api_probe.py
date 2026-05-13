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
      inputFields {
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
      enumValues {
        name
      }
    }
  }
}
'''

TOKENS_QUERY = '''
query ApiProbeTokens($input: PublicTokenListInput!) {
  tokens(input: $input) {
    items {
      id
      name
      symbol
      speedMode
      initialPrice
      price
      startDate
      endDate
      buysCount
      sellsCount
      uniqueTradersCount
      volumeUsdtDrops
      rank
    }
    meta {
      cursor
      hasNextPage
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
    print('\nAvailable Query fields:')
    for field in fields:
        args = field.get('args') or []
        arg_text = ', '.join(f"{a.get('name')}: {compact_type(a.get('type'))}" for a in args)
        if arg_text:
            print(f"- {field.get('name')}({arg_text})")
        else:
            print(f"- {field.get('name')}")
    return fields


def schema_maps(type_data):
    types = type_data.get('data', {}).get('__schema', {}).get('types') or []
    by_name = {}
    for item in types:
        name = item.get('name')
        if name:
            by_name[name] = item
    return by_name


def print_named_type(by_name, name):
    item = by_name.get(name)
    if not item:
        print(f'- {name}: not found')
        return
    print(f'\n{name} [{item.get("kind")}]')
    for field in item.get('inputFields') or []:
        print(f"- {field.get('name')}: {compact_type(field.get('type'))}")
    for value in item.get('enumValues') or []:
        print(f"- {value.get('name')}")
    for field in item.get('fields') or []:
        print(f"- {field.get('name')}")


def print_token_related_types(by_name):
    print('\nToken/turbo/price related schema types:')
    matches = []
    for name, item in by_name.items():
        if any(word in name.lower() for word in ('token', 'turbo', 'price', 'stream')):
            field_names = [f.get('name') for f in (item.get('fields') or []) if f.get('name')]
            input_names = [f.get('name') for f in (item.get('inputFields') or []) if f.get('name')]
            enum_names = [v.get('name') for v in (item.get('enumValues') or []) if v.get('name')]
            shown = field_names or input_names or enum_names
            matches.append((name, item.get('kind'), shown[:20]))
    for name, kind, shown in matches[:100]:
        suffix = ''
        if shown:
            suffix = ' fields=' + ', '.join(shown)
        print(f'- {name} [{kind}]{suffix}')


def try_tokens_examples(headers, by_name):
    input_type = by_name.get('PublicTokenListInput') or {}
    input_fields = [f.get('name') for f in (input_type.get('inputFields') or [])]
    print('\nTrying read-only tokens(input: ...) examples')
    print(f'PublicTokenListInput fields: {input_fields}')

    candidates = []
    if 'first' in input_fields:
        candidates.append({'first': 5})
    if 'limit' in input_fields:
        candidates.append({'limit': 5})
    if 'take' in input_fields:
        candidates.append({'take': 5})
    if 'pagination' in input_fields:
        candidates.append({'pagination': {'first': 5}})
    candidates.append({})

    seen = set()
    unique_candidates = []
    for candidate in candidates:
        key = json.dumps(candidate, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique_candidates.append(candidate)

    for candidate in unique_candidates:
        print('\ninput candidate:')
        print(json.dumps(candidate, ensure_ascii=False, indent=2))
        try:
            response = post_graphql(
                headers,
                TOKENS_QUERY,
                variables={'input': candidate},
                operation_name='ApiProbeTokens',
            )
            print(f'status={response.status_code}')
            print(response.text[:3000])
            data = response.json()
            if response.status_code == 200 and not data.get('errors'):
                print('\nSUCCESS: tokens query works with this input')
                return candidate
        except Exception as exc:
            print(f'tokens candidate failed: {exc}')
    return None


print(f'API_URL={API_URL}')
print(f'TOKEN_ID={TOKEN_ID}')
print('This probe is read-only. It uses GraphQL introspection and tokens query only.')

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

        type_response = post_graphql(headers, TYPE_QUERY, operation_name='ApiProbeTypes')
        type_data = type_response.json()
        if type_response.status_code == 200 and not type_data.get('errors'):
            by_name = schema_maps(type_data)
            print_named_type(by_name, 'PublicTokenListInput')
            print_named_type(by_name, 'TurboTokenListFilterInput')
            print_named_type(by_name, 'TurboTokenListSortInput')
            print_named_type(by_name, 'TurboTokenListSorting')
            print_named_type(by_name, 'TurboTokenMode')
            print_named_type(by_name, 'PublicTokenListOutput')
            print_token_related_types(by_name)
            try_tokens_examples(headers, by_name)
        else:
            print('Type introspection failed:')
            print(type_response.text[:2000])
        break

    errors = data.get('errors') if isinstance(data, dict) else None
    if errors:
        print('\nerrors preview:')
        print(json.dumps(errors, ensure_ascii=False, indent=2)[:1600])
else:
    print('\nNo header candidate succeeded. Check API key scopes and endpoint.')
