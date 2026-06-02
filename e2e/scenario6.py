import uuid
import time
import subprocess

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ConsistencyLevel, SimpleStatement

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
    time.sleep(4)


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


def demo_consistency_levels(sku, zone):
    print('\n=== Step 10: Demo CL=ONE vs QUORUM vs ALL (with cassandra-2 stopped) ===')
    cluster = Cluster(
        CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
    )
    session = cluster.connect(KEYSPACE)

    for cl_name, cl_value in [
        ('ONE',    ConsistencyLevel.ONE),
        ('QUORUM', ConsistencyLevel.QUORUM),
        ('ALL',    ConsistencyLevel.ALL),
    ]:
        stmt = SimpleStatement(
            'SELECT available FROM inventory_by_product_zone WHERE sku=%s AND zone_id=%s',
            consistency_level=cl_value,
        )
        try:
            row = session.execute(stmt, (sku, zone)).one()
            avail = row.available if row else 0
            print(f'  [OK]   CL={cl_name}: available={avail}')
        except Exception as exc:
            print(f'  [FAIL] CL={cl_name}: {type(exc).__name__}: {exc}')

    cluster.shutdown()


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

    print('\n=== Step 2: Check cluster status (3 nodes UN) ===')
    result = subprocess.run(
        ['docker', 'exec', 'cassandrakafka-cassandra-1-1', 'nodetool', 'status'],
        capture_output=True, text=True,
    )
    print(result.stdout or result.stderr)

    print('\n=== Step 3: PRODUCT_RECEIVED qty=200 ===')
    ev = base_event('PRODUCT_RECEIVED')
    ev.update({'sku': sku, 'zone_id': zone, 'quantity': 200, 'product_name': 'Test Product'})
    send(producer, ev)

    print('\n=== Step 4: Check available=200 ===')
    avail, _ = get_inv(session, sku, zone)
    check('available after RECEIVED', avail, 200)

    print('\n=== Step 5: Stopping cassandra-2 ===')
    subprocess.run(['docker', 'stop', 'cassandrakafka-cassandra-2-1'], check=True)
    print('  cassandra-2 stopped. Waiting 5s for cluster to detect...')
    time.sleep(5)

    print('\n=== Step 6: PRODUCT_SHIPPED qty=50 (with cassandra-2 down) ===')
    ev = base_event('PRODUCT_SHIPPED')
    ev.update({'sku': sku, 'zone_id': zone, 'quantity': 50})
    send(producer, ev)

    print('\n=== Step 7: Check available=150 (system works without cassandra-2) ===')
    avail, _ = get_inv(session, sku, zone)
    check('available after SHIPPED (1 node down)', avail, 150)

    demo_consistency_levels(sku, zone)

    print('\n=== Step 8: Starting cassandra-2 back ===')
    subprocess.run(['docker', 'start', 'cassandrakafka-cassandra-2-1'], check=True)
    print('  cassandra-2 started. Waiting 30s for rejoin...')
    time.sleep(30)

    print('\n=== Step 9: Check cluster status (3 nodes UN again) ===')
    result = subprocess.run(
        ['docker', 'exec', 'cassandrakafka-cassandra-1-1', 'nodetool', 'status'],
        capture_output=True, text=True,
    )
    print(result.stdout or result.stderr)

    print('\n=== ALL CHECKS PASSED ===')
    cluster.shutdown()


if __name__ == '__main__':
    main()
