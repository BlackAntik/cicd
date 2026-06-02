import os
import time
import json
import traceback
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

from confluent_kafka import DeserializingConsumer, Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, SimpleStatement, BatchType, ConsistencyLevel

from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

EVENTS_PROCESSED = Counter(
    'events_processed_total',
    'Total number of processed events',
    ['event_type'],
)
PROCESSING_DURATION = Histogram(
    'event_processing_duration_seconds',
    'Time spent processing a single event',
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)
CASSANDRA_WRITE_ERRORS = Counter(
    'cassandra_write_errors_total',
    'Total number of Cassandra write errors',
)
CONSUMER_LAG = Gauge(
    'consumer_lag',
    'Consumer lag per partition',
    ['topic', 'partition'],
)
HTTP_REQUESTS_TOTAL = Counter(
    'http_requests_total',
    'Total number of HTTP requests to the metrics/health server',
    ['method', 'endpoint', 'status'],
)
HTTP_REQUEST_ERRORS_TOTAL = Counter(
    'http_request_errors_total',
    'Total number of HTTP request errors',
    ['method', 'endpoint', 'error_type'],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint'],
    buckets=[0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

_health = {'kafka': False, 'cassandra': False}


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        method = 'GET'
        endpoint = self.path.split('?')[0]
        start = time.time()
        status_code = 200
        error_type = None

        try:
            if endpoint == '/metrics':
                data = generate_latest()
                self.send_response(200)
                self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                self.end_headers()
                self.wfile.write(data)
            elif endpoint == '/health':
                if _health['kafka'] and _health['cassandra']:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'OK')
                else:
                    status_code = 503
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(json.dumps(_health).encode())
            else:
                status_code = 404
                error_type = 'not_found'
                self.send_response(404)
                self.end_headers()
        except Exception as exc:
            status_code = 500
            error_type = type(exc).__name__
            raise
        finally:
            duration = time.time() - start
            HTTP_REQUESTS_TOTAL.labels(
                method=method,
                endpoint=endpoint,
                status=str(status_code),
            ).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(
                method=method,
                endpoint=endpoint,
            ).observe(duration)
            if error_type is not None:
                HTTP_REQUEST_ERRORS_TOTAL.labels(
                    method=method,
                    endpoint=endpoint,
                    error_type=error_type,
                ).inc()

    def log_message(self, fmt, *args):
        pass


def start_http_server(port=8000):
    server = HTTPServer(('0.0.0.0', port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info('Metrics server started on port %d', port)


def connect_cassandra(hosts, port, keyspace, retries=20, delay=5):
    host_list = [h.strip() for h in hosts.split(',')]
    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster(
                host_list,
                port=int(port),
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
            )
            session = cluster.connect(keyspace)
            session.default_consistency_level = ConsistencyLevel.QUORUM
            logger.info('Connected to Cassandra keyspace: %s hosts: %s', keyspace, host_list)
            return session
        except Exception as exc:
            logger.warning('Cassandra connection attempt %d/%d failed: %s', attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError('Could not connect to Cassandra after %d attempts' % retries)


def get_inventory(session, sku, zone_id):
    row = session.execute(
        'SELECT available, reserved, last_event_timestamp '
        'FROM inventory_by_product_zone WHERE sku=%s AND zone_id=%s',
        (sku, zone_id),
    ).one()
    if row:
        return row.available or 0, row.reserved or 0, row.last_event_timestamp or 0
    return 0, 0, 0


def _compute_totals(session, sku, zone_id, new_available, new_reserved):
    rows = session.execute(
        'SELECT zone_id, available, reserved FROM inventory_by_product_zone WHERE sku=%s',
        (sku,),
    )
    total_avail = 0
    total_res = 0
    seen_current = False
    for row in rows:
        if row.zone_id == zone_id:
            seen_current = True
            total_avail += new_available or 0
            total_res += new_reserved or 0
        else:
            total_avail += row.available or 0
            total_res += row.reserved or 0
    if not seen_current:
        total_avail += new_available or 0
        total_res += new_reserved or 0
    return total_avail, total_res


def write_inventory(session, sku, zone_id, available, reserved, now, event_ts, supplier_id=None):
    total_available, total_reserved = _compute_totals(session, sku, zone_id, available, reserved)

    batch = BatchStatement(batch_type=BatchType.LOGGED)
    for table, col1, col2, v1, v2 in [
        ('inventory_by_product_zone', 'sku',     'zone_id', sku,     zone_id),
        ('inventory_by_zone',         'zone_id', 'sku',     zone_id, sku),
    ]:
        batch.add(
            SimpleStatement(
                f'INSERT INTO {table} ({col1}, {col2}, available, reserved, updated_at, last_event_timestamp, supplier_id) '
                f'VALUES (%s, %s, %s, %s, %s, %s, %s)'
            ),
            (v1, v2, available, reserved, now, event_ts, supplier_id),
        )
    batch.add(
        SimpleStatement(
            'INSERT INTO inventory_by_product (sku, total_available, total_reserved, updated_at, supplier_id) '
            'VALUES (%s, %s, %s, %s, %s)'
        ),
        (sku, total_available, total_reserved, now, supplier_id),
    )
    session.execute(batch)


def append_event_log(session, event, now):
    sku = event.get('sku')
    if not sku:
        return
    session.execute(
        'INSERT INTO event_log (sku, occurred_at, event_id, event_type, zone_id, quantity, order_id) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s)',
        (
            sku,
            now,
            event.get('event_id'),
            event.get('event_type'),
            event.get('zone_id') or event.get('source_zone_id'),
            event.get('quantity'),
            event.get('order_id'),
        ),
    )


def is_stale(event_ts, last_ts, event_id='', sku='', zone_id=''):
    if event_ts <= last_ts:
        logger.info(
            'Stale event ignored event_id=%s sku=%s zone_id=%s event_ts=%d last_ts=%d',
            event_id, sku, zone_id, event_ts, last_ts,
        )
        return True
    return False


def handle_product_received(session, event, now):
    sku = event['sku']
    zone_id = event['zone_id']
    qty = event.get('quantity') or 0
    event_ts = event.get('timestamp') or 0
    event_id = event.get('event_id', '')
    supplier_id = event.get('supplier_id')
    avail, res, last_ts = get_inventory(session, sku, zone_id)
    if is_stale(event_ts, last_ts, event_id, sku, zone_id):
        return
    write_inventory(session, sku, zone_id, avail + qty, res, now, event_ts, supplier_id)


def handle_product_shipped(session, event, now):
    sku = event['sku']
    zone_id = event['zone_id']
    qty = event.get('quantity') or 0
    event_ts = event.get('timestamp') or 0
    event_id = event.get('event_id', '')
    avail, res, last_ts = get_inventory(session, sku, zone_id)
    if is_stale(event_ts, last_ts, event_id, sku, zone_id):
        return
    write_inventory(session, sku, zone_id, avail - qty, res, now, event_ts)


def handle_product_moved(session, event, now):
    sku = event['sku']
    src = event['source_zone_id']
    dst = event['destination_zone_id']
    qty = event.get('quantity') or 0
    event_ts = event.get('timestamp') or 0
    event_id = event.get('event_id', '')

    avail_src, res_src, last_ts_src = get_inventory(session, sku, src)
    if not is_stale(event_ts, last_ts_src, event_id, sku, src):
        write_inventory(session, sku, src, avail_src - qty, res_src, now, event_ts)

    avail_dst, res_dst, last_ts_dst = get_inventory(session, sku, dst)
    if not is_stale(event_ts, last_ts_dst, event_id, sku, dst):
        write_inventory(session, sku, dst, avail_dst + qty, res_dst, now, event_ts)


def handle_product_reserved(session, event, now):
    sku = event['sku']
    zone_id = event['zone_id']
    qty = event.get('quantity') or 0
    event_ts = event.get('timestamp') or 0
    event_id = event.get('event_id', '')
    avail, res, last_ts = get_inventory(session, sku, zone_id)
    if is_stale(event_ts, last_ts, event_id, sku, zone_id):
        return
    write_inventory(session, sku, zone_id, avail - qty, res + qty, now, event_ts)


def handle_product_released(session, event, now):
    sku = event['sku']
    zone_id = event['zone_id']
    qty = event.get('quantity') or 0
    event_ts = event.get('timestamp') or 0
    event_id = event.get('event_id', '')
    avail, res, last_ts = get_inventory(session, sku, zone_id)
    if is_stale(event_ts, last_ts, event_id, sku, zone_id):
        return
    write_inventory(session, sku, zone_id, avail + qty, res - qty, now, event_ts)


def handle_inventory_counted(session, event, now):
    sku = event['sku']
    zone_id = event['zone_id']
    qty = event.get('quantity') or 0
    event_ts = event.get('timestamp') or 0
    event_id = event.get('event_id', '')
    _, _, last_ts = get_inventory(session, sku, zone_id)
    if is_stale(event_ts, last_ts, event_id, sku, zone_id):
        return
    write_inventory(session, sku, zone_id, qty, 0, now, event_ts)


def handle_order_created(session, event, now):
    order_id = event.get('order_id')
    items_raw = event.get('items') or ''

    session.execute(
        'INSERT INTO orders (order_id, status, items, created_at, updated_at) '
        'VALUES (%s, %s, %s, %s, %s) IF NOT EXISTS',
        (order_id, 'CREATED', items_raw, now, now),
    )

    try:
        items = json.loads(items_raw)
    except (ValueError, TypeError):
        items = []
        for part in items_raw.split(';'):
            part = part.strip()
            if ':' in part:
                sku, qty_str = part.split(':', 1)
                try:
                    items.append({'sku': sku.strip(), 'zone_id': 'ZONE-A1', 'quantity': int(qty_str.strip())})
                except ValueError:
                    pass

    event_ts = event.get('timestamp') or 0
    for item in items:
        sku = item.get('sku')
        zone_id = item.get('zone_id', 'ZONE-A1')
        qty = item.get('quantity', 0)
        if sku and qty:
            avail, res, last_ts = get_inventory(session, sku, zone_id)
            if not is_stale(event_ts, last_ts):
                write_inventory(session, sku, zone_id, avail - qty, res + qty, now, event_ts)


def handle_order_completed(session, event, now):
    order_id = event.get('order_id')

    row = session.execute(
        'SELECT items FROM orders WHERE order_id=%s',
        (order_id,),
    ).one()

    session.execute(
        'UPDATE orders SET status=%s, updated_at=%s WHERE order_id=%s',
        ('COMPLETED', now, order_id),
    )

    if not row:
        return

    items_raw = row.items or ''
    try:
        items = json.loads(items_raw)
    except (ValueError, TypeError):
        items = []
        for part in items_raw.split(';'):
            part = part.strip()
            if ':' in part:
                sku, qty_str = part.split(':', 1)
                try:
                    items.append({'sku': sku.strip(), 'zone_id': 'ZONE-A1', 'quantity': int(qty_str.strip())})
                except ValueError:
                    pass

    event_ts = event.get('timestamp') or 0
    for item in items:
        sku = item.get('sku')
        zone_id = item.get('zone_id', 'ZONE-A1')
        qty = item.get('quantity', 0)
        if sku and qty:
            avail, res, last_ts = get_inventory(session, sku, zone_id)
            if not is_stale(event_ts, last_ts):
                write_inventory(session, sku, zone_id, avail, res - qty, now, event_ts)


HANDLERS = {
    'PRODUCT_RECEIVED':  handle_product_received,
    'PRODUCT_SHIPPED':   handle_product_shipped,
    'PRODUCT_MOVED':     handle_product_moved,
    'PRODUCT_RESERVED':  handle_product_reserved,
    'PRODUCT_RELEASED':  handle_product_released,
    'INVENTORY_COUNTED': handle_inventory_counted,
    'ORDER_CREATED':     handle_order_created,
    'ORDER_COMPLETED':   handle_order_completed,
}


def process_event(session, event):
    now = datetime.now(timezone.utc)
    event_type = event.get('event_type')
    handler = HANDLERS.get(event_type)
    if handler:
        handler(session, event, now)
    else:
        logger.warning('Unknown event_type: %s', event_type)
    append_event_log(session, event, now)


def record_processed(session, event_id, event_type):
    now = datetime.now(timezone.utc)
    session.execute(
        'INSERT INTO processed_events (event_id, event_type, processed_at) VALUES (%s, %s, %s)',
        (event_id, event_type, now),
    )


class ValidationError(Exception):
    def __init__(self, message, code='VALIDATION_ERROR'):
        super().__init__(message)
        self.code = code


def validate_event(event):
    event_type = event.get('event_type')
    qty = event.get('quantity')

    if event_type in ('PRODUCT_RECEIVED', 'PRODUCT_SHIPPED', 'PRODUCT_MOVED',
                      'PRODUCT_RESERVED', 'PRODUCT_RELEASED', 'INVENTORY_COUNTED'):
        if qty is not None and qty < 0:
            raise ValidationError(f'Invalid quantity: {qty} (must be non-negative)', 'VALIDATION_ERROR')

    if event_type in ('PRODUCT_RECEIVED', 'PRODUCT_SHIPPED', 'PRODUCT_RESERVED',
                      'PRODUCT_RELEASED', 'INVENTORY_COUNTED'):
        if not event.get('sku'):
            raise ValidationError('Missing required field: sku', 'VALIDATION_ERROR')
        if not event.get('zone_id'):
            raise ValidationError('Missing required field: zone_id', 'VALIDATION_ERROR')

    if event_type == 'PRODUCT_MOVED':
        if not event.get('sku'):
            raise ValidationError('Missing required field: sku', 'VALIDATION_ERROR')
        if not event.get('source_zone_id'):
            raise ValidationError('Missing required field: source_zone_id', 'VALIDATION_ERROR')
        if not event.get('destination_zone_id'):
            raise ValidationError('Missing required field: destination_zone_id', 'VALIDATION_ERROR')


def send_to_dlq(dlq_producer, dlq_topic, event, exc, partition, offset):
    failed_at = datetime.now(timezone.utc).isoformat()
    error_code = getattr(exc, 'code', 'PROCESSING_ERROR')
    dlq_message = {
        'original_event': event,
        'error_reason': str(exc),
        'error_code': error_code,
        'failed_at': failed_at,
        'kafka_metadata': {
            'partition': partition,
            'offset': offset,
        },
    }
    dlq_producer.produce(
        topic=dlq_topic,
        value=json.dumps(dlq_message).encode('utf-8'),
    )
    dlq_producer.flush()
    logger.warning(
        'Sent to DLQ event_id=%s error_code=%s reason=%s',
        event.get('event_id', 'unknown'), error_code, str(exc),
    )


def update_consumer_lag(consumer, topic):
    try:
        assignment = consumer.assignment()
        for tp in assignment:
            low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
            committed = consumer.committed([tp], timeout=1.0)
            committed_offset = committed[0].offset if committed and committed[0].offset >= 0 else low
            lag = max(0, high - committed_offset)
            CONSUMER_LAG.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
    except Exception as exc:
        logger.debug('Failed to update consumer lag: %s', exc)


def main():
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    schema_registry_url = os.environ.get('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
    topic = os.environ.get('KAFKA_TOPIC', 'warehouse-events')
    dlq_topic = os.environ.get('KAFKA_DLQ_TOPIC', 'warehouse-events-dlq')
    group_id = os.environ.get('KAFKA_GROUP_ID', 'warehouse-state-consumer')
    cassandra_hosts = os.environ.get('CASSANDRA_HOSTS', 'cassandra-1,cassandra-2,cassandra-3')
    cassandra_port = os.environ.get('CASSANDRA_PORT', '9042')
    cassandra_keyspace = os.environ.get('CASSANDRA_KEYSPACE', 'warehouse')
    metrics_port = int(os.environ.get('METRICS_PORT', '8000'))

    start_http_server(metrics_port)

    session = connect_cassandra(cassandra_hosts, cassandra_port, cassandra_keyspace)
    _health['cassandra'] = True

    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer = DeserializingConsumer({
        'bootstrap.servers': bootstrap_servers,
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })

    dlq_producer = Producer({'bootstrap.servers': bootstrap_servers})

    consumer.subscribe([topic])
    _health['kafka'] = True
    logger.info('Consumer started. Group: %s, Topic: %s', group_id, topic)

    lag_update_counter = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            lag_update_counter += 1
            if lag_update_counter >= 10:
                update_consumer_lag(consumer, topic)
                lag_update_counter = 0

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug('Reached end of partition %d', msg.partition())
                else:
                    _health['kafka'] = False
                    raise KafkaException(msg.error())
                continue

            event = msg.value()
            event_id = event.get('event_id', 'unknown')
            event_type = event.get('event_type', 'unknown')
            offset = msg.offset()
            partition = msg.partition()

            try:
                already = session.execute(
                    'SELECT event_id FROM processed_events WHERE event_id=%s',
                    (event_id,),
                ).one()

                if already:
                    logger.info(
                        'Skipping duplicate event_id=%s event_type=%s partition=%d offset=%d',
                        event_id, event_type, partition, offset,
                    )
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                validate_event(event)

                start_time = time.time()
                try:
                    process_event(session, event)
                except Exception as cassandra_exc:
                    CASSANDRA_WRITE_ERRORS.inc()
                    raise cassandra_exc
                duration = time.time() - start_time
                PROCESSING_DURATION.observe(duration)

                record_processed(session, event_id, event_type)
                EVENTS_PROCESSED.labels(event_type=event_type).inc()
                consumer.commit(message=msg, asynchronous=False)
                logger.info(
                    'Processed event_id=%s event_type=%s partition=%d offset=%d duration=%.3fs',
                    event_id, event_type, partition, offset, duration,
                )
            except Exception as exc:
                logger.error(
                    'Failed to process event_id=%s event_type=%s partition=%d offset=%d: %s',
                    event_id, event_type, partition, offset, exc,
                )
                try:
                    send_to_dlq(dlq_producer, dlq_topic, event, exc, partition, offset)
                except Exception as dlq_exc:
                    logger.error('Failed to send to DLQ: %s', dlq_exc)
                consumer.commit(message=msg, asynchronous=False)
    finally:
        _health['kafka'] = False
        consumer.close()
        logger.info('Consumer closed')


if __name__ == '__main__':
    main()
