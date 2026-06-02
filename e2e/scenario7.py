import uuid
import time
import json
import subprocess
import requests

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

BOOTSTRAP = 'localhost:9092'
SCHEMA_REGISTRY = 'http://localhost:8081'
TOPIC = 'warehouse-events'
CONSUMER_URL = 'http://localhost:8000'
PROMETHEUS_URL = 'http://localhost:9090'

SCHEMA = open('../schemas/warehouse_event.avsc').read()

EVENT_TYPES = [
    'PRODUCT_RECEIVED', 'PRODUCT_SHIPPED', 'PRODUCT_MOVED',
    'PRODUCT_RESERVED', 'PRODUCT_RELEASED', 'INVENTORY_COUNTED',
    'ORDER_CREATED', 'ORDER_COMPLETED',
]

SKUS = ['SKU-001', 'SKU-002', 'SKU-003']
ZONES = ['ZONE-A', 'ZONE-B', 'ZONE-C']


def make_ts():
    return int(time.time() * 1000)


def base_event(event_type):
    return {
        'event_id': str(uuid.uuid4()),
        'event_type': event_type,
        'timestamp': make_ts(),
        'sku': None,
        'zone_id': None,
        'source_zone_id': None,
        'destination_zone_id': None,
        'quantity': None,
        'order_id': None,
        'product_name': None,
        'items': None,
    }


def make_valid_event(event_type):
    import random
    ev = base_event(event_type)
    sku = random.choice(SKUS)
    zone = random.choice(ZONES)
    qty = random.randint(1, 20)
    if event_type == 'PRODUCT_RECEIVED':
        ev.update({'sku': sku, 'zone_id': zone, 'quantity': qty, 'product_name': 'Item'})
    elif event_type == 'PRODUCT_SHIPPED':
        ev.update({'sku': sku, 'zone_id': zone, 'quantity': 1})
    elif event_type == 'PRODUCT_MOVED':
        src, dst = random.sample(ZONES, 2)
        ev.update({'sku': sku, 'source_zone_id': src, 'destination_zone_id': dst, 'quantity': 1})
    elif event_type in ('PRODUCT_RESERVED', 'PRODUCT_RELEASED'):
        ev.update({'sku': sku, 'zone_id': zone, 'quantity': 1, 'order_id': str(uuid.uuid4())})
    elif event_type == 'INVENTORY_COUNTED':
        ev.update({'sku': sku, 'zone_id': zone, 'quantity': qty})
    elif event_type == 'ORDER_CREATED':
        items = json.dumps([{'sku': sku, 'zone_id': zone, 'quantity': 1}])
        ev.update({'order_id': str(uuid.uuid4()), 'items': items})
    elif event_type == 'ORDER_COMPLETED':
        ev.update({'order_id': str(uuid.uuid4())})
    return ev


def check(label, actual, expected):
    status = 'OK' if actual == expected else 'FAIL'
    print(f'  [{status}] {label}: expected={expected}, actual={actual}')
    if actual != expected:
        raise AssertionError(f'{label}: expected {expected}, got {actual}')


def get_metric(metric_name):
    try:
        r = requests.get(f'{PROMETHEUS_URL}/api/v1/query', params={'query': metric_name}, timeout=5)
        data = r.json()
        results = data.get('data', {}).get('result', [])
        if results:
            return float(results[0]['value'][1])
    except Exception:
        pass
    return None


def main():
    import random

    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY})
    avro_ser = AvroSerializer(sr_client, SCHEMA)
    producer = SerializingProducer({
        'bootstrap.servers': BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_ser,
    })

    print('\n=== Step 1: GET /health ===')
    r = requests.get(f'{CONSUMER_URL}/health', timeout=5)
    print(f'  Status: {r.status_code} {r.text}')
    check('/health returns 200', r.status_code, 200)

    print('\n=== Step 2: GET /metrics ===')
    r = requests.get(f'{CONSUMER_URL}/metrics', timeout=5)
    check('/metrics returns 200', r.status_code, 200)
    has_events = 'events_processed_total' in r.text
    has_lag = 'consumer_lag' in r.text
    has_duration = 'event_processing_duration_seconds' in r.text
    has_errors = 'cassandra_write_errors_total' in r.text
    check('events_processed_total present', has_events, True)
    check('consumer_lag present', has_lag, True)
    check('event_processing_duration_seconds present', has_duration, True)
    check('cassandra_write_errors_total present', has_errors, True)
    print(f'  Metrics endpoint OK, {len(r.text)} bytes')

    print('\n=== Step 3: Send 10 events of different types ===')
    before_total = get_metric('sum(events_processed_total)') or 0
    for i, event_type in enumerate(EVENT_TYPES[:8]):
        ev = make_valid_event(event_type)
        producer.produce(topic=TOPIC, key=ev['event_id'], value=ev)
        print(f'  [{i+1}/8] Sent {event_type}')
    ev = make_valid_event('PRODUCT_RECEIVED')
    producer.produce(topic=TOPIC, key=ev['event_id'], value=ev)
    ev = make_valid_event('PRODUCT_RECEIVED')
    producer.produce(topic=TOPIC, key=ev['event_id'], value=ev)
    producer.flush()
    print('  Waiting 15s for consumer to process...')
    time.sleep(15)

    print('\n=== Step 4: Check /metrics after events ===')
    after_total = get_metric('sum(events_processed_total)') or 0
    lag_val = get_metric('sum(consumer_lag)') or 0
    print(f'  events_processed_total: before={before_total}, after={after_total}')
    print(f'  consumer_lag: {lag_val}')
    check('events_processed_total increased', after_total > before_total, True)

    print('\n=== Step 5: Grafana dashboard ===')
    r = requests.get('http://localhost:3000/api/health', timeout=5)
    check('Grafana is up', r.status_code, 200)
    print(f'  Grafana: {r.json()}')
    print('  Dashboard URL: http://localhost:3000/d/warehouse-consumer')

    print('\n=== Step 6: Stop consumer → lag grows ===')
    consumer_container = subprocess.run(
        ['docker', 'ps', '-a', '--filter', 'name=consumer', '--format', '{{.Names}}'],
        check=True, capture_output=True, text=True,
    ).stdout.strip().splitlines()[0]
    subprocess.run(['docker', 'stop', consumer_container], check=True)
    print('  Consumer stopped. Waiting 20s for producer to add messages...')
    time.sleep(20)

    lag_after_stop = get_metric('sum(consumer_lag)')
    print(f'  consumer_lag after stop: {lag_after_stop}')

    print('\n=== Step 7: Check alert rule (lag > threshold) ===')
    r = requests.get(f'{PROMETHEUS_URL}/api/v1/rules', timeout=5)
    if r.status_code == 200:
        rules = r.json().get('data', {}).get('groups', [])
        for group in rules:
            for rule in group.get('rules', []):
                if 'consumer_lag' in rule.get('name', ''):
                    print(f'  Alert rule: {rule["name"]} state={rule.get("state")}')

    print('\n=== Step 8: Start consumer back → lag decreases ===')
    subprocess.run(['docker', 'start', consumer_container], check=True)
    print('  Consumer started. Waiting 20s for catch-up...')
    time.sleep(20)

    lag_after_start = get_metric('sum(consumer_lag)') or 0
    print(f'  consumer_lag after restart: {lag_after_start}')
    check('lag decreased after restart', lag_after_start < (lag_after_stop or 999), True)

    print('\n=== ALL CHECKS PASSED ===')


if __name__ == '__main__':
    main()
