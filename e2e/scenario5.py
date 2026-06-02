import uuid
import time
import json

from confluent_kafka import SerializingProducer, Consumer as KafkaConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ConsistencyLevel

BOOTSTRAP = 'localhost:9092'
SCHEMA_REGISTRY = 'http://localhost:8081'
TOPIC = 'warehouse-events'
DLQ_TOPIC = 'warehouse-events-dlq'
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
    print(f'  Sent {event["event_type"]} event_id={event["event_id"]} quantity={event.get("quantity")}')
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


def read_dlq(invalid_event_id, timeout=10):
    dlq_consumer = KafkaConsumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': f'e2e-dlq-reader-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    dlq_consumer.subscribe([DLQ_TOPIC])
    deadline = time.time() + timeout
    found = None
    while time.time() < deadline:
        msg = dlq_consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            orig = data.get('original_event', {})
            if orig.get('event_id') == invalid_event_id:
                found = data
                break
        except Exception:
            continue
    dlq_consumer.close()
    return found


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

    print('\n=== Step 1: Send INVALID event PRODUCT_SHIPPED quantity=-5 ===')
    invalid_ev = base_event('PRODUCT_SHIPPED')
    invalid_ev.update({'sku': sku, 'zone_id': zone, 'quantity': -5})
    invalid_event_id = invalid_ev['event_id']
    send(producer, invalid_ev)

    print('\n=== Step 2: Check consumer is still alive (send probe event) ===')
    probe_ev = base_event('PRODUCT_RECEIVED')
    probe_ev.update({'sku': sku, 'zone_id': zone, 'quantity': 1, 'product_name': 'Probe'})
    send(producer, probe_ev)
    avail, _ = get_inv(session, sku, zone)
    check('consumer alive (probe event processed)', avail, 1)

    print(f'\n=== Step 3: Check DLQ contains invalid event_id={invalid_event_id} ===')
    dlq_msg = read_dlq(invalid_event_id, timeout=15)
    check('DLQ message found', dlq_msg is not None, True)
    if dlq_msg:
        print(f'  DLQ error_code: {dlq_msg.get("error_code")}')
        print(f'  DLQ error_reason: {dlq_msg.get("error_reason")}')
        print(f'  DLQ failed_at: {dlq_msg.get("failed_at")}')
        print(f'  DLQ partition: {dlq_msg.get("kafka_metadata", {}).get("partition")}')
        print(f'  DLQ offset: {dlq_msg.get("kafka_metadata", {}).get("offset")}')
        check('error_code is VALIDATION_ERROR', dlq_msg.get('error_code'), 'VALIDATION_ERROR')

    print('\n=== Step 4: Send valid event after invalid ===')
    valid_ev = base_event('PRODUCT_RECEIVED')
    valid_ev.update({'sku': sku, 'zone_id': zone, 'quantity': 50, 'product_name': 'Test Product'})
    send(producer, valid_ev)

    print('\n=== Step 5: Check valid event processed correctly ===')
    avail, _ = get_inv(session, sku, zone)
    check('available after valid event', avail, 51)

    print('\n=== ALL CHECKS PASSED ===')
    cluster.shutdown()


if __name__ == '__main__':
    main()
