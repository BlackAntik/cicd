import uuid
import time

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

SCHEMA = open('../schemas/warehouse_event.avsc').read()


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


def send(producer, event):
    producer.produce(topic=TOPIC, key=event['event_id'], value=event)
    producer.flush()
    print(f'  Sent {event["event_type"]} event_id={event["event_id"]}')
    time.sleep(3)


def get_inv(session, sku, zone_id):
    row = session.execute(
        'SELECT available, reserved FROM inventory_by_product_zone WHERE sku=%s AND zone_id=%s',
        (sku, zone_id),
    ).one()
    if row:
        return row.available or 0, row.reserved or 0
    return 0, 0


def check(label, actual, expected):
    status = 'OK' if actual == expected else 'FAIL'
    print(f'  [{status}] {label}: expected={expected}, actual={actual}')
    if actual != expected:
        raise AssertionError(f'{label}: expected {expected}, got {actual}')


def main():
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY})
    avro_ser = AvroSerializer(sr_client, SCHEMA)
    producer = SerializingProducer({
        'bootstrap.servers': BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_ser,
    })

    cluster = Cluster(
        CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
    )
    session = cluster.connect(KEYSPACE)
    session.default_consistency_level = ConsistencyLevel.QUORUM

    test_id = str(uuid.uuid4())[:8]
    sku = f'TEST-{test_id}'
    zone = f'ZONE-TEST-{test_id}'
    print(f'\nUsing sku={sku}, zone={zone}')

    print('\n=== Step 1: PRODUCT_RECEIVED qty=50 (first time) ===')
    ev = base_event('PRODUCT_RECEIVED')
    ev.update({'sku': sku, 'zone_id': zone, 'quantity': 50, 'product_name': 'Test Product'})
    event_id = ev['event_id']
    send(producer, ev)

    print('\n=== Step 2: check available=50 ===')
    avail, res = get_inv(session, sku, zone)
    check('available after first send', avail, 50)

    print(f'\n=== Step 3: Resend SAME event (event_id={event_id}) ===')
    ev_duplicate = dict(ev)
    ev_duplicate['event_id'] = event_id
    send(producer, ev_duplicate)

    print('\n=== Step 4: check available still=50 (not 100) ===')
    avail, res = get_inv(session, sku, zone)
    check('available after duplicate send', avail, 50)

    print('\n=== ALL CHECKS PASSED ===')
    cluster.shutdown()


if __name__ == '__main__':
    main()
