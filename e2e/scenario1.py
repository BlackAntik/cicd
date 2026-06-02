import uuid
import time
import json

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
    zone_a = f'ZONE-TEST-A-{test_id}'
    zone_b = f'ZONE-TEST-B-{test_id}'
    print(f'\nUsing sku={sku}, zone_a={zone_a}, zone_b={zone_b}')

    print('\n=== Step 2: PRODUCT_RECEIVED SKU-001 ZONE-A qty=100 ===')
    ev = base_event('PRODUCT_RECEIVED')
    ev.update({'sku': sku, 'zone_id': zone_a, 'quantity': 100, 'product_name': 'Widget Alpha'})
    send(producer, ev)

    print('\n=== Step 3: inventory_by_product_zone ZONE-A ===')
    avail, res = get_inv(session, sku, zone_a)
    check('ZONE-A available', avail, 100)
    check('ZONE-A reserved', res, 0)

    print('\n=== Step 4: inventory_by_product total ===')
    row = session.execute(
        'SELECT total_available FROM inventory_by_product WHERE sku=%s', (sku,)
    ).one()
    total = row.total_available if row else 0
    check('total available', total, 100)

    print('\n=== Step 5: PRODUCT_RESERVED SKU-001 ZONE-A qty=30 ===')
    ev = base_event('PRODUCT_RESERVED')
    ev.update({'sku': sku, 'zone_id': zone_a, 'quantity': 30, 'order_id': str(uuid.uuid4())})
    send(producer, ev)

    print('\n=== Step 6: after RESERVED ===')
    avail, res = get_inv(session, sku, zone_a)
    check('ZONE-A available', avail, 70)
    check('ZONE-A reserved', res, 30)

    print('\n=== Step 7: PRODUCT_MOVED SKU-001 ZONE-A->ZONE-B qty=20 ===')
    ev = base_event('PRODUCT_MOVED')
    ev.update({'sku': sku, 'source_zone_id': zone_a, 'destination_zone_id': zone_b, 'quantity': 20})
    send(producer, ev)

    print('\n=== Step 8: after MOVED ===')
    avail_a, _ = get_inv(session, sku, zone_a)
    avail_b, _ = get_inv(session, sku, zone_b)
    check('ZONE-A available', avail_a, 50)
    check('ZONE-B available', avail_b, 20)

    print('\n=== Step 9: PRODUCT_SHIPPED SKU-001 ZONE-A qty=10 ===')
    ev = base_event('PRODUCT_SHIPPED')
    ev.update({'sku': sku, 'zone_id': zone_a, 'quantity': 10})
    send(producer, ev)

    print('\n=== Step 10: after SHIPPED ===')
    avail_a, _ = get_inv(session, sku, zone_a)
    check('ZONE-A available', avail_a, 40)

    order_id = str(uuid.uuid4())
    items = json.dumps([{'sku': sku, 'zone_id': zone_a, 'quantity': 15}])

    print(f'\n=== Step 11: ORDER_CREATED order_id={order_id} SKU-001 qty=15 ===')
    ev = base_event('ORDER_CREATED')
    ev.update({'order_id': order_id, 'items': items})
    send(producer, ev)

    print('\n=== Step 12: after ORDER_CREATED ===')
    avail_a, res_a = get_inv(session, sku, zone_a)
    check('ZONE-A available', avail_a, 25)
    check('ZONE-A reserved', res_a, 45)

    print(f'\n=== Step 13: ORDER_COMPLETED order_id={order_id} ===')
    ev = base_event('ORDER_COMPLETED')
    ev.update({'order_id': order_id})
    send(producer, ev)

    print('\n=== Step 14: after ORDER_COMPLETED ===')
    avail_a, res_a = get_inv(session, sku, zone_a)
    check('ZONE-A available', avail_a, 25)
    check('ZONE-A reserved', res_a, 30)

    print('\n=== ALL CHECKS PASSED ===')
    cluster.shutdown()


if __name__ == '__main__':
    main()
