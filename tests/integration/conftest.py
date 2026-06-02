import os
import uuid
import time
import pytest

from confluent_kafka import SerializingProducer
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
EVENT_PROPAGATION_TIMEOUT = int(os.environ.get('EVENT_PROPAGATION_TIMEOUT', '10'))

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), '../../schemas/warehouse_event.avsc')
SCHEMA_V1 = open(SCHEMA_PATH).read()

SCHEMA_V2_PATH = os.path.join(os.path.dirname(__file__), '../../schemas/warehouse_event_v2.avsc')
SCHEMA_V2 = open(SCHEMA_V2_PATH).read()


@pytest.fixture(scope='session')
def cassandra_cluster():
    cluster = Cluster(
        CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
        connect_timeout=30,
    )
    yield cluster
    cluster.shutdown()


@pytest.fixture(scope='session')
def cassandra_session(cassandra_cluster):
    session = cassandra_cluster.connect(CASSANDRA_KEYSPACE)
    session.default_consistency_level = ConsistencyLevel.QUORUM
    return session


@pytest.fixture(scope='session')
def schema_registry_client():
    return SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})


@pytest.fixture(scope='session')
def avro_producer_v1(schema_registry_client):
    avro_ser = AvroSerializer(schema_registry_client, SCHEMA_V1)
    prod = SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_ser,
    })
    yield prod
    prod.flush()


@pytest.fixture(scope='session')
def avro_producer_v2(schema_registry_client):
    avro_ser = AvroSerializer(schema_registry_client, SCHEMA_V2)
    prod = SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_ser,
    })
    yield prod
    prod.flush()


@pytest.fixture
def test_sku():
    return f'IT-{str(uuid.uuid4())[:8]}'


@pytest.fixture
def test_zone():
    return f'ZONE-{str(uuid.uuid4())[:8]}'


@pytest.fixture
def test_order_id():
    return str(uuid.uuid4())


@pytest.fixture(autouse=True)
def cleanup_cassandra(cassandra_session, request):
    tracked_skus = []
    tracked_zones = []
    tracked_order_ids = []
    tracked_event_ids = []

    def track(skus=None, zones=None, order_ids=None, event_ids=None):
        if skus:
            tracked_skus.extend(skus)
        if zones:
            tracked_zones.extend(zones)
        if order_ids:
            tracked_order_ids.extend(order_ids)
        if event_ids:
            tracked_event_ids.extend(event_ids)

    request.node._track = track

    yield

    for sku in tracked_skus:
        cassandra_session.execute(
            'DELETE FROM inventory_by_product WHERE sku=%s', (sku,)
        )
        cassandra_session.execute(
            'DELETE FROM inventory_by_product_zone WHERE sku=%s', (sku,)
        )
        cassandra_session.execute(
            'DELETE FROM event_log WHERE sku=%s', (sku,)
        )

    for zone in tracked_zones:
        cassandra_session.execute(
            'DELETE FROM inventory_by_zone WHERE zone_id=%s', (zone,)
        )

    for order_id in tracked_order_ids:
        cassandra_session.execute(
            'DELETE FROM orders WHERE order_id=%s', (order_id,)
        )

    for event_id in tracked_event_ids:
        cassandra_session.execute(
            'DELETE FROM processed_events WHERE event_id=%s', (event_id,)
        )


def send_event(producer, event, timeout=10):
    delivered = []
    errors = []

    def on_delivery(err, msg):
        if err:
            errors.append(err)
        else:
            delivered.append(msg)

    producer.produce(
        topic=KAFKA_TOPIC,
        key=event['event_id'],
        value=event,
        on_delivery=on_delivery,
    )
    producer.flush(timeout=timeout)
    if errors:
        raise RuntimeError(f'Kafka delivery failed: {errors}')
    return delivered


def make_event(event_type, **kwargs):
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


def wait_for(condition_fn, timeout=None, interval=0.5):
    if timeout is None:
        timeout = EVENT_PROPAGATION_TIMEOUT
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition_fn()
        if result is not None:
            return result
        time.sleep(interval)
    return condition_fn()


def get_inventory(session, sku, zone_id):
    row = session.execute(
        'SELECT available, reserved FROM inventory_by_product_zone '
        'WHERE sku=%s AND zone_id=%s',
        (sku, zone_id),
    ).one()
    if row:
        return row.available or 0, row.reserved or 0
    return None, None


def wait_for_inventory(session, sku, zone_id, expected_available, expected_reserved=None, timeout=None):
    def check():
        avail, res = get_inventory(session, sku, zone_id)
        if avail is None:
            return None
        if avail != expected_available:
            return None
        if expected_reserved is not None and res != expected_reserved:
            return None
        return (avail, res)

    return wait_for(check, timeout=timeout)
