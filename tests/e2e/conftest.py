import os
import uuid
import time
import json
import requests
import pytest

from confluent_kafka import SerializingProducer, Consumer as KafkaConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ConsistencyLevel

KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
CASSANDRA_HOSTS = os.environ.get('CASSANDRA_HOSTS', 'localhost').split(',')
CASSANDRA_PORT = int(os.environ.get('CASSANDRA_PORT', '9042'))
CASSANDRA_KEYSPACE = os.environ.get('CASSANDRA_KEYSPACE', 'warehouse')
CONSUMER_URL = os.environ.get('CONSUMER_URL', 'http://localhost:8000')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'warehouse-events')
KAFKA_DLQ_TOPIC = os.environ.get('KAFKA_DLQ_TOPIC', 'warehouse-events-dlq')
EVENT_PROPAGATION_TIMEOUT = int(os.environ.get('EVENT_PROPAGATION_TIMEOUT', '15'))

SCHEMA_V1_PATH = os.path.join(os.path.dirname(__file__), '../../schemas/warehouse_event.avsc')
SCHEMA_V2_PATH = os.path.join(os.path.dirname(__file__), '../../schemas/warehouse_event_v2.avsc')
SCHEMA_V1 = open(SCHEMA_V1_PATH).read()
SCHEMA_V2 = open(SCHEMA_V2_PATH).read()


@pytest.fixture(scope='session')
def cassandra_session():
    cluster = Cluster(
        CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
        connect_timeout=30,
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.default_consistency_level = ConsistencyLevel.QUORUM
    yield session
    cluster.shutdown()


@pytest.fixture(scope='session')
def sr_client():
    return SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})


@pytest.fixture(scope='session')
def producer_v1(sr_client):
    prod = SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': AvroSerializer(sr_client, SCHEMA_V1),
    })
    yield prod
    prod.flush()


@pytest.fixture(scope='session')
def producer_v2(sr_client):
    prod = SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': AvroSerializer(sr_client, SCHEMA_V2),
    })
    yield prod
    prod.flush()


def publish(producer, event, timeout=10):
    errors = []

    def on_delivery(err, msg):
        if err:
            errors.append(err)

    producer.produce(
        topic=KAFKA_TOPIC,
        key=event['event_id'],
        value=event,
        on_delivery=on_delivery,
    )
    producer.flush(timeout=timeout)
    if errors:
        raise RuntimeError(f'Kafka delivery error: {errors}')


def new_event(event_type, **kwargs):
    ev = {
        'event_id': str(uuid.uuid4()),
        'event_type': event_type,
        'timestamp': int(time.time() * 1000),
        'sku': None,
        'zone_id': None,
        'source_zone_id': None,
        'destination_zone_id': None,
        'quantity': None,
        'order_id': None,
        'product_name': None,
        'items': None,
    }
    ev.update(kwargs)
    return ev


def poll_cassandra(fn, timeout=None):
    if timeout is None:
        timeout = EVENT_PROPAGATION_TIMEOUT
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = fn()
        if result is not None:
            return result
        time.sleep(0.5)
    return fn()


def get_inv(session, sku, zone):
    row = session.execute(
        'SELECT available, reserved FROM inventory_by_product_zone '
        'WHERE sku=%s AND zone_id=%s', (sku, zone)
    ).one()
    return (row.available or 0, row.reserved or 0) if row else (None, None)


def wait_inv(session, sku, zone, avail=None, reserved=None, timeout=None):
    def check():
        a, r = get_inv(session, sku, zone)
        if a is None:
            return None
        if avail is not None and a != avail:
            return None
        if reserved is not None and r != reserved:
            return None
        return (a, r)
    return poll_cassandra(check, timeout=timeout)


def read_dlq_message(event_id, timeout=20):
    consumer = KafkaConsumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': f'e2e-dlq-{uuid.uuid4().hex[:8]}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([KAFKA_DLQ_TOPIC])
    deadline = time.time() + timeout
    found = None
    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            if data.get('original_event', {}).get('event_id') == event_id:
                found = data
                break
        except Exception:
            continue
    consumer.close()
    return found


def get_metrics_text():
    resp = requests.get(f'{CONSUMER_URL}/metrics', timeout=5)
    assert resp.status_code == 200
    return resp.text


def get_counter_value(metrics_text, event_type):
    for line in metrics_text.splitlines():
        if line.startswith('events_processed_total{') and event_type in line:
            try:
                return float(line.split()[-1])
            except (ValueError, IndexError):
                pass
    return 0.0
