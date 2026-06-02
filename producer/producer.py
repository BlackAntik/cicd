import os
import time
import uuid
import json
import random
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SCHEMA_V1 = open('/app/schemas/warehouse_event.avsc').read()
SCHEMA_V2 = open('/app/schemas/warehouse_event_v2.avsc').read()

SKUS = ['SKU-001', 'SKU-002', 'SKU-003', 'SKU-004', 'SKU-005']
ZONES = ['ZONE-A1', 'ZONE-A2', 'ZONE-B1', 'ZONE-B2', 'ZONE-C1']
SUPPLIERS = ['SUP-001', 'SUP-002', 'SUP-003']
PRODUCT_NAMES = {
    'SKU-001': 'Widget Alpha',
    'SKU-002': 'Widget Beta',
    'SKU-003': 'Gadget X',
    'SKU-004': 'Gadget Y',
    'SKU-005': 'Component Z',
}

KAFKA_MESSAGES_PRODUCED_TOTAL = Counter(
    'kafka_messages_produced_total',
    'Total number of messages successfully produced to Kafka',
    ['topic', 'event_type'],
)
KAFKA_PRODUCE_ERRORS_TOTAL = Counter(
    'kafka_produce_errors_total',
    'Total number of Kafka produce errors',
    ['topic', 'error_type'],
)
KAFKA_PRODUCE_DURATION_SECONDS = Histogram(
    'kafka_produce_duration_seconds',
    'Time spent producing a single message to Kafka',
    ['topic', 'event_type'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)
HTTP_REQUESTS_TOTAL = Counter(
    'http_requests_total',
    'Total number of HTTP requests to the metrics server',
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


class ProducerMetricsHandler(BaseHTTPRequestHandler):
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
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'OK')
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


def start_metrics_server(port=8001):
    server = HTTPServer(('0.0.0.0', port), ProducerMetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info('Producer metrics server started on port %d', port)


def make_event(event_type, use_v2=False):
    now_ms = int(time.time() * 1000)
    event_id = str(uuid.uuid4())
    sku = random.choice(SKUS)
    zone = random.choice(ZONES)
    qty = random.randint(1, 50)

    base = {
        'event_id': event_id,
        'event_type': event_type,
        'timestamp': now_ms,
        'sku': None,
        'zone_id': None,
        'source_zone_id': None,
        'destination_zone_id': None,
        'quantity': None,
        'order_id': None,
        'product_name': None,
        'items': None,
    }

    if use_v2:
        base['supplier_id'] = random.choice(SUPPLIERS)

    if event_type == 'PRODUCT_RECEIVED':
        base.update({'sku': sku, 'zone_id': zone, 'quantity': qty, 'product_name': PRODUCT_NAMES[sku]})
    elif event_type == 'PRODUCT_SHIPPED':
        base.update({'sku': sku, 'zone_id': zone, 'quantity': qty})
    elif event_type == 'PRODUCT_MOVED':
        src, dst = random.sample(ZONES, 2)
        base.update({'sku': sku, 'source_zone_id': src, 'destination_zone_id': dst, 'quantity': qty})
    elif event_type == 'PRODUCT_RESERVED':
        base.update({'sku': sku, 'zone_id': zone, 'quantity': qty, 'order_id': str(uuid.uuid4())})
    elif event_type == 'PRODUCT_RELEASED':
        base.update({'sku': sku, 'zone_id': zone, 'quantity': qty, 'order_id': str(uuid.uuid4())})
    elif event_type == 'INVENTORY_COUNTED':
        base.update({'sku': sku, 'zone_id': zone, 'quantity': qty})
    elif event_type == 'ORDER_CREATED':
        order_id = str(uuid.uuid4())
        num_items = random.randint(1, 3)
        items = [
            {'sku': random.choice(SKUS), 'zone_id': random.choice(ZONES), 'quantity': random.randint(1, 10)}
            for _ in range(num_items)
        ]
        base.update({'order_id': order_id, 'items': json.dumps(items)})
    elif event_type == 'ORDER_COMPLETED':
        base.update({'order_id': str(uuid.uuid4())})

    return base


def delivery_report(err, msg):
    if err is not None:
        logger.error('Delivery failed for event %s: %s', msg.key(), err)
    else:
        logger.info('Event delivered to %s [%d] offset %d', msg.topic(), msg.partition(), msg.offset())


def main():
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    schema_registry_url = os.environ.get('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
    topic = os.environ.get('KAFKA_TOPIC', 'warehouse-events')
    metrics_port = int(os.environ.get('METRICS_PORT', '8001'))

    start_metrics_server(metrics_port)

    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

    avro_serializer_v1 = AvroSerializer(schema_registry_client, SCHEMA_V1)
    avro_serializer_v2 = AvroSerializer(schema_registry_client, SCHEMA_V2)

    producer_v1 = SerializingProducer({
        'bootstrap.servers': bootstrap_servers,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer_v1,
    })

    producer_v2 = SerializingProducer({
        'bootstrap.servers': bootstrap_servers,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer_v2,
    })

    event_types = [
        'PRODUCT_RECEIVED',
        'PRODUCT_SHIPPED',
        'PRODUCT_MOVED',
        'PRODUCT_RESERVED',
        'PRODUCT_RELEASED',
        'INVENTORY_COUNTED',
        'ORDER_CREATED',
        'ORDER_COMPLETED',
    ]

    logger.info('Producer started, publishing to topic: %s', topic)

    counter = 0
    while True:
        counter += 1
        if counter % 10 == 0:
            event = make_event('PRODUCT_RECEIVED')
            event['quantity'] = -5
            use_v2 = False
        else:
            use_v2 = (counter % 3 == 0)
            event_type = random.choice(event_types)
            event = make_event(event_type, use_v2=use_v2)

        producer = producer_v2 if use_v2 else producer_v1
        event_type = event['event_type']
        start = time.time()

        def make_delivery_callback(ev_type, tp, st):
            def callback(err, msg):
                duration = time.time() - st
                if err is not None:
                    logger.error('Delivery failed for event %s: %s', msg.key(), err)
                    KAFKA_PRODUCE_ERRORS_TOTAL.labels(
                        topic=tp,
                        error_type=type(err).__name__,
                    ).inc()
                else:
                    logger.info('Event delivered to %s [%d] offset %d',
                                msg.topic(), msg.partition(), msg.offset())
                    KAFKA_MESSAGES_PRODUCED_TOTAL.labels(
                        topic=tp,
                        event_type=ev_type,
                    ).inc()
                    KAFKA_PRODUCE_DURATION_SECONDS.labels(
                        topic=tp,
                        event_type=ev_type,
                    ).observe(duration)
            return callback

        producer.produce(
            topic=topic,
            key=event['event_id'],
            value=event,
            on_delivery=make_delivery_callback(event_type, topic, start),
        )
        producer.poll(0)
        time.sleep(2)


if __name__ == '__main__':
    main()
