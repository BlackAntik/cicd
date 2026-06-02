import os
import json
import time
import requests

SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
SUBJECT = 'warehouse-events-value'


def wait_for_registry(retries=20, delay=5):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f'{SCHEMA_REGISTRY_URL}/subjects', timeout=5)
            if r.status_code == 200:
                return
        except Exception:
            pass
        print(f'Waiting for Schema Registry... attempt {attempt}/{retries}')
        time.sleep(delay)
    raise RuntimeError('Schema Registry not available')


def set_compatibility(subject, level):
    r = requests.put(
        f'{SCHEMA_REGISTRY_URL}/config/{subject}',
        json={'compatibility': level},
    )
    r.raise_for_status()
    print(f'Set compatibility {level} for subject {subject}')


def register_schema(subject, schema_path):
    schema_str = open(schema_path).read()
    r = requests.post(
        f'{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions',
        json={'schema': schema_str},
        headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'},
    )
    r.raise_for_status()
    schema_id = r.json()['id']
    print(f'Registered schema from {schema_path} with id={schema_id}')
    return schema_id


def main():
    wait_for_registry()
    set_compatibility(SUBJECT, 'BACKWARD')
    register_schema(SUBJECT, '/app/schemas/warehouse_event.avsc')
    register_schema(SUBJECT, '/app/schemas/warehouse_event_v2.avsc')
    print('Schema registration complete')


if __name__ == '__main__':
    main()
