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

    print('\n=== Step 1: PRODUCT_RECEIVED qty=100 ===')
    ev = base_event('PRODUCT_RECEIVED')
    ev.update({'sku': sku, 'zone_id': zone, 'quantity': 100, 'product_name': 'Test Product'})
    send(producer, ev)

    print('\n=== Step 2a: inventory_by_product_zone ===')
    row = session.execute(
        'SELECT available, reserved FROM inventory_by_product_zone WHERE sku=%s AND zone_id=%s',
        (sku, zone),
    ).one()
    avail_pz = row.available if row else 0
    res_pz = row.reserved if row else 0
    check('inventory_by_product_zone available', avail_pz, 100)
    check('inventory_by_product_zone reserved', res_pz, 0)

    print('\n=== Step 2b: inventory_by_product ===')
    row = session.execute(
        'SELECT total_available, total_reserved FROM inventory_by_product WHERE sku=%s',
        (sku,),
    ).one()
    total_avail = row.total_available if row else 0
    total_res = row.total_reserved if row else 0
    check('inventory_by_product total_available', total_avail, 100)
    check('inventory_by_product total_reserved', total_res, 0)

    print('\n=== Step 2c: inventory_by_zone ===')
    rows_z = list(session.execute(
        'SELECT sku, available, reserved FROM inventory_by_zone WHERE zone_id=%s',
        (zone,),
    ))
    sku_row = next((r for r in rows_z if r.sku == sku), None)
    avail_z = sku_row.available if sku_row else 0
    check('inventory_by_zone contains sku', sku_row is not None, True)
    check('inventory_by_zone available', avail_z, 100)

    print('\n=== Step 3: All three tables consistent ===')
    check('pz == p == z', avail_pz == total_avail == avail_z, True)

    print('\n=== ALL CHECKS PASSED ===')
    cluster.shutdown()


if __name__ == '__main__':
    main()
