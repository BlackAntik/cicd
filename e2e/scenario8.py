import uuid
import time
import requests

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ConsistencyLevel

BOOTSTRAP = 'localhost:9092'
SCHEMA_REGISTRY = 'http://localhost:8081'
TOPIC = 'warehouse-events'
CASSANDRA_HOSTS = ['localhost']
CASSANDRA_PORT = 9042
KEYSPACE = 'warehouse'

SCHEMA_V1 = open('../schemas/warehouse_event.avsc').read()
SCHEMA_V2 = open('../schemas/warehouse_event_v2.avsc').read()


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


def send(producer, event, label):
    producer.produce(topic=TOPIC, key=event['event_id'], value=event)
    producer.flush()
    print(f'  Sent [{label}] {event["event_type"]} event_id={event["event_id"]}')
    time.sleep(4)


def check(label, actual, expected):
    status = 'OK' if actual == expected else 'FAIL'
    print(f'  [{status}] {label}: expected={repr(expected)}, actual={repr(actual)}')
    if actual != expected:
        raise AssertionError(f'{label}: expected {expected!r}, got {actual!r}')


def get_inv_full(session, sku, zone_id):
    row = session.execute(
        'SELECT available, reserved, supplier_id FROM inventory_by_product_zone WHERE sku=%s AND zone_id=%s',
        (sku, zone_id),
    ).one()
    if row:
        return row.available or 0, row.reserved or 0, row.supplier_id
    return 0, 0, None


def main():
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY})

    avro_ser_v1 = AvroSerializer(sr_client, SCHEMA_V1)
    avro_ser_v2 = AvroSerializer(sr_client, SCHEMA_V2)

    producer_v1 = SerializingProducer({
        'bootstrap.servers': BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_ser_v1,
    })
    producer_v2 = SerializingProducer({
        'bootstrap.servers': BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_ser_v2,
    })

    cluster = Cluster(
        CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
    )
    session = cluster.connect(KEYSPACE)
    session.default_consistency_level = ConsistencyLevel.QUORUM

    test_id = str(uuid.uuid4())[:8]
    sku_v1 = f'TEST-V1-{test_id}'
    sku_v2 = f'TEST-V2-{test_id}'
    zone = f'ZONE-TEST-{test_id}'
    print(f'\nUsing sku_v1={sku_v1}, sku_v2={sku_v2}, zone={zone}')

    print('\n=== Step 1: Send V1 event PRODUCT_RECEIVED (no supplier_id) ===')
    ev_v1 = base_event('PRODUCT_RECEIVED')
    ev_v1.update({'sku': sku_v1, 'zone_id': zone, 'quantity': 100, 'product_name': 'Widget V1'})
    send(producer_v1, ev_v1, 'V1')

    print('\n=== Step 2: Check V1 event processed ===')
    avail, res, supplier = get_inv_full(session, sku_v1, zone)
    check('V1 available', avail, 100)
    check('V1 supplier_id is None', supplier, None)

    print('\n=== Step 3: Send V2 event PRODUCT_RECEIVED (with supplier_id=SUP-001) ===')
    ev_v2 = base_event('PRODUCT_RECEIVED')
    ev_v2.update({
        'sku': sku_v2, 'zone_id': zone, 'quantity': 200,
        'product_name': 'Widget V2', 'supplier_id': 'SUP-001',
    })
    send(producer_v2, ev_v2, 'V2')

    print('\n=== Step 4: Check V2 event processed ===')
    avail, res, supplier = get_inv_full(session, sku_v2, zone)
    check('V2 available', avail, 200)
    check('V2 supplier_id', supplier, 'SUP-001')

    print('\n=== Step 5: Verify V1 record has supplier_id=None ===')
    _, _, supplier_v1 = get_inv_full(session, sku_v1, zone)
    check('V1 supplier_id is None (default)', supplier_v1, None)

    print('\n=== Step 6: Verify V2 record has supplier_id=SUP-001 ===')
    _, _, supplier_v2 = get_inv_full(session, sku_v2, zone)
    check('V2 supplier_id=SUP-001', supplier_v2, 'SUP-001')

    print('\n=== Step 7: Schema Registry versions ===')
    subject = 'warehouse-events-value'
    r = requests.get(f'{SCHEMA_REGISTRY}/subjects/{subject}/versions', timeout=5)
    versions = r.json()
    print(f'  Subject: {subject}')
    print(f'  Versions: {versions}')
    check('at least 2 versions registered', len(versions) >= 2, True)

    r_compat = requests.get(f'{SCHEMA_REGISTRY}/config/{subject}', timeout=5)
    compat = r_compat.json().get('compatibilityLevel', 'UNKNOWN')
    print(f'  Compatibility: {compat}')
    check('compatibility is BACKWARD', compat, 'BACKWARD')

    for v in versions:
        r_v = requests.get(f'{SCHEMA_REGISTRY}/subjects/{subject}/versions/{v}', timeout=5)
        schema_info = r_v.json()
        print(f'  Version {v}: id={schema_info.get("id")}')

    print('\n=== ALL CHECKS PASSED ===')
    cluster.shutdown()


if __name__ == '__main__':
    main()
