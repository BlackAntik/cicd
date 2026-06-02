import json
import time
import uuid
import requests
import pytest

from tests.integration.conftest import (
    send_event,
    make_event,
    wait_for_inventory,
    wait_for,
    get_inventory,
    CONSUMER_URL,
    SCHEMA_REGISTRY_URL,
    EVENT_PROPAGATION_TIMEOUT,
)


class TestKafkaToConsumerToCassandra:
    """
    Проверяет сквозной путь: Kafka → Consumer → Cassandra.
    Каждый тест использует уникальные SKU/zone, изолирован и очищает состояние.
    """

    def test_product_received_persisted_in_all_three_tables(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                        quantity=80, product_name='Widget')
        send_event(avro_producer_v1, ev)

        result = wait_for_inventory(cassandra_session, sku, zone, 80, 0)
        assert result is not None, f'inventory_by_product_zone not updated: sku={sku} zone={zone}'

        row_zone = cassandra_session.execute(
            'SELECT available FROM inventory_by_zone WHERE zone_id=%s AND sku=%s',
            (zone, sku),
        ).one()
        assert row_zone is not None, 'inventory_by_zone not updated'
        assert row_zone.available == 80

        row_product = cassandra_session.execute(
            'SELECT total_available FROM inventory_by_product WHERE sku=%s', (sku,)
        ).one()
        assert row_product is not None, 'inventory_by_product not updated'
        assert row_product.total_available == 80

    def test_event_written_to_event_log(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                        quantity=10, product_name='Widget')
        send_event(avro_producer_v1, ev)

        wait_for_inventory(cassandra_session, sku, zone, 10)

        def check_log():
            rows = list(cassandra_session.execute(
                'SELECT event_id, event_type FROM event_log WHERE sku=%s', (sku,)
            ))
            return rows if rows else None

        rows = wait_for(check_log)
        assert rows is not None, 'event_log not populated'
        assert any(r.event_type == 'PRODUCT_RECEIVED' for r in rows)

    def test_event_recorded_in_processed_events(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                        quantity=5, product_name='Widget')
        event_id = ev['event_id']
        request.node._track(event_ids=[event_id])
        send_event(avro_producer_v1, ev)

        wait_for_inventory(cassandra_session, sku, zone, 5)

        def check_processed():
            row = cassandra_session.execute(
                'SELECT event_id FROM processed_events WHERE event_id=%s', (event_id,)
            ).one()
            return row if row else None

        row = wait_for(check_processed)
        assert row is not None, f'processed_events missing event_id={event_id}'

    def test_product_shipped_kafka_to_cassandra(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])
        ts = int(time.time() * 1000)

        ev_recv = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                             quantity=100, product_name='Widget', timestamp=ts)
        send_event(avro_producer_v1, ev_recv)
        wait_for_inventory(cassandra_session, sku, zone, 100)

        ev_ship = make_event('PRODUCT_SHIPPED', sku=sku, zone_id=zone,
                             quantity=35, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_ship)

        result = wait_for_inventory(cassandra_session, sku, zone, 65)
        assert result is not None, f'Expected available=65 after PRODUCT_SHIPPED, got {get_inventory(cassandra_session, sku, zone)}'

    def test_product_moved_kafka_to_cassandra_two_zones(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone_src = f'ZS-{uuid.uuid4().hex[:8]}'
        zone_dst = f'ZD-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone_src, zone_dst])
        ts = int(time.time() * 1000)

        ev_recv = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone_src,
                             quantity=100, product_name='Widget', timestamp=ts)
        send_event(avro_producer_v1, ev_recv)
        wait_for_inventory(cassandra_session, sku, zone_src, 100)

        ev_move = make_event('PRODUCT_MOVED', sku=sku,
                             source_zone_id=zone_src, destination_zone_id=zone_dst,
                             quantity=40, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_move)

        src_result = wait_for_inventory(cassandra_session, sku, zone_src, 60)
        dst_result = wait_for_inventory(cassandra_session, sku, zone_dst, 40)

        assert src_result is not None, f'zone_src: expected 60, got {get_inventory(cassandra_session, sku, zone_src)}'
        assert dst_result is not None, f'zone_dst: expected 40, got {get_inventory(cassandra_session, sku, zone_dst)}'

    def test_product_reserved_kafka_to_cassandra(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        order_id = str(uuid.uuid4())
        request.node._track(skus=[sku], zones=[zone], order_ids=[order_id])
        ts = int(time.time() * 1000)

        ev_recv = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                             quantity=100, product_name='Widget', timestamp=ts)
        send_event(avro_producer_v1, ev_recv)
        wait_for_inventory(cassandra_session, sku, zone, 100)

        ev_res = make_event('PRODUCT_RESERVED', sku=sku, zone_id=zone,
                            quantity=30, order_id=order_id, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_res)

        result = wait_for_inventory(cassandra_session, sku, zone, 70, 30)
        assert result is not None, \
            f'Expected available=70 reserved=30, got {get_inventory(cassandra_session, sku, zone)}'

    def test_product_released_kafka_to_cassandra(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        order_id = str(uuid.uuid4())
        request.node._track(skus=[sku], zones=[zone], order_ids=[order_id])
        ts = int(time.time() * 1000)

        ev_recv = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                             quantity=100, product_name='Widget', timestamp=ts)
        send_event(avro_producer_v1, ev_recv)
        wait_for_inventory(cassandra_session, sku, zone, 100)

        ev_res = make_event('PRODUCT_RESERVED', sku=sku, zone_id=zone,
                            quantity=40, order_id=order_id, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_res)
        wait_for_inventory(cassandra_session, sku, zone, 60, 40)

        ev_rel = make_event('PRODUCT_RELEASED', sku=sku, zone_id=zone,
                            quantity=40, order_id=order_id, timestamp=ts + 2000)
        send_event(avro_producer_v1, ev_rel)

        result = wait_for_inventory(cassandra_session, sku, zone, 100, 0)
        assert result is not None, \
            f'Expected available=100 reserved=0 after PRODUCT_RELEASED, got {get_inventory(cassandra_session, sku, zone)}'

    def test_inventory_counted_sets_absolute_value(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])
        ts = int(time.time() * 1000)

        ev_recv = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                             quantity=999, product_name='Widget', timestamp=ts)
        send_event(avro_producer_v1, ev_recv)
        wait_for_inventory(cassandra_session, sku, zone, 999)

        ev_count = make_event('INVENTORY_COUNTED', sku=sku, zone_id=zone,
                              quantity=42, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_count)

        result = wait_for_inventory(cassandra_session, sku, zone, 42, 0)
        assert result is not None, \
            f'Expected available=42 after INVENTORY_COUNTED, got {get_inventory(cassandra_session, sku, zone)}'


class TestOrderLifecycle:
    """
    Проверяет сквозной путь ORDER_CREATED → ORDER_COMPLETED через Kafka → Consumer → Cassandra.
    """

    def test_order_created_reserves_items_in_cassandra(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        order_id = str(uuid.uuid4())
        request.node._track(skus=[sku], zones=[zone], order_ids=[order_id])
        ts = int(time.time() * 1000)

        ev_recv = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                             quantity=100, product_name='Widget', timestamp=ts)
        send_event(avro_producer_v1, ev_recv)
        wait_for_inventory(cassandra_session, sku, zone, 100)

        items = json.dumps([{'sku': sku, 'zone_id': zone, 'quantity': 25}])
        ev_order = make_event('ORDER_CREATED', order_id=order_id,
                              items=items, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_order)

        result = wait_for_inventory(cassandra_session, sku, zone, 75, 25)
        assert result is not None, \
            f'Expected available=75 reserved=25 after ORDER_CREATED, got {get_inventory(cassandra_session, sku, zone)}'

        row_order = cassandra_session.execute(
            'SELECT status FROM orders WHERE order_id=%s', (order_id,)
        ).one()
        assert row_order is not None, 'Order not persisted in orders table'
        assert row_order.status == 'CREATED'

    def test_order_completed_releases_reserved_in_cassandra(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        order_id = str(uuid.uuid4())
        request.node._track(skus=[sku], zones=[zone], order_ids=[order_id])
        ts = int(time.time() * 1000)

        ev_recv = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                             quantity=100, product_name='Widget', timestamp=ts)
        send_event(avro_producer_v1, ev_recv)
        wait_for_inventory(cassandra_session, sku, zone, 100)

        items = json.dumps([{'sku': sku, 'zone_id': zone, 'quantity': 20}])
        ev_order = make_event('ORDER_CREATED', order_id=order_id,
                              items=items, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_order)
        wait_for_inventory(cassandra_session, sku, zone, 80, 20)

        ev_complete = make_event('ORDER_COMPLETED', order_id=order_id,
                                 timestamp=ts + 2000)
        send_event(avro_producer_v1, ev_complete)

        result = wait_for_inventory(cassandra_session, sku, zone, 80, 0)
        assert result is not None, \
            f'Expected reserved=0 after ORDER_COMPLETED, got {get_inventory(cassandra_session, sku, zone)}'

        def check_order_status():
            row = cassandra_session.execute(
                'SELECT status FROM orders WHERE order_id=%s', (order_id,)
            ).one()
            return row if row and row.status == 'COMPLETED' else None

        row = wait_for(check_order_status)
        assert row is not None, 'Order status not updated to COMPLETED'

    def test_full_order_lifecycle_multi_sku(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku1 = f'IT-{uuid.uuid4().hex[:8]}'
        sku2 = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        order_id = str(uuid.uuid4())
        request.node._track(skus=[sku1, sku2], zones=[zone], order_ids=[order_id])
        ts = int(time.time() * 1000)

        for sku, qty in [(sku1, 50), (sku2, 80)]:
            ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                            quantity=qty, product_name='Widget',
                            timestamp=ts)
            send_event(avro_producer_v1, ev)
            ts += 100

        wait_for_inventory(cassandra_session, sku1, zone, 50)
        wait_for_inventory(cassandra_session, sku2, zone, 80)

        items = json.dumps([
            {'sku': sku1, 'zone_id': zone, 'quantity': 10},
            {'sku': sku2, 'zone_id': zone, 'quantity': 15},
        ])
        ev_order = make_event('ORDER_CREATED', order_id=order_id,
                              items=items, timestamp=ts + 1000)
        send_event(avro_producer_v1, ev_order)

        wait_for_inventory(cassandra_session, sku1, zone, 40, 10)
        wait_for_inventory(cassandra_session, sku2, zone, 65, 15)

        ev_complete = make_event('ORDER_COMPLETED', order_id=order_id,
                                 timestamp=ts + 2000)
        send_event(avro_producer_v1, ev_complete)

        r1 = wait_for_inventory(cassandra_session, sku1, zone, 40, 0)
        r2 = wait_for_inventory(cassandra_session, sku2, zone, 65, 0)
        assert r1 is not None, f'sku1 reserved not released: {get_inventory(cassandra_session, sku1, zone)}'
        assert r2 is not None, f'sku2 reserved not released: {get_inventory(cassandra_session, sku2, zone)}'


class TestIdempotency:
    """
    Проверяет, что дублирующееся событие не обрабатывается дважды.
    Путь: Kafka (дубль) → Consumer (проверка processed_events) → Cassandra (без изменений).
    """

    def test_duplicate_event_not_applied_twice(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                        quantity=50, product_name='Widget')
        request.node._track(event_ids=[ev['event_id']])

        send_event(avro_producer_v1, ev)
        wait_for_inventory(cassandra_session, sku, zone, 50)

        send_event(avro_producer_v1, ev)
        time.sleep(EVENT_PROPAGATION_TIMEOUT)

        avail, _ = get_inventory(cassandra_session, sku, zone)
        assert avail == 50, f'Idempotency violated: expected 50, got {avail}'

    def test_duplicate_event_recorded_once_in_processed_events(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                        quantity=10, product_name='Widget')
        event_id = ev['event_id']
        request.node._track(event_ids=[event_id])

        send_event(avro_producer_v1, ev)
        wait_for_inventory(cassandra_session, sku, zone, 10)

        send_event(avro_producer_v1, ev)
        time.sleep(EVENT_PROPAGATION_TIMEOUT)

        rows = list(cassandra_session.execute(
            'SELECT event_id FROM processed_events WHERE event_id=%s', (event_id,)
        ))
        assert len(rows) == 1, f'Expected 1 record in processed_events, got {len(rows)}'


class TestDLQAndRecovery:
    """
    Проверяет, что невалидное событие уходит в DLQ и consumer продолжает работу.
    Путь: Kafka (невалидное) → Consumer (DLQ) → Consumer (следующее валидное) → Cassandra.
    """

    def test_invalid_event_sent_to_dlq_consumer_stays_healthy(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev_bad = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                            quantity=-100, product_name='Bad Widget')
        send_event(avro_producer_v1, ev_bad)
        time.sleep(3)

        resp = requests.get(f'{CONSUMER_URL}/health', timeout=5)
        assert resp.status_code == 200, \
            f'Consumer unhealthy after invalid event: {resp.status_code} {resp.text}'

    def test_consumer_processes_valid_event_after_invalid(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev_bad = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                            quantity=-1, product_name='Bad')
        send_event(avro_producer_v1, ev_bad)
        time.sleep(2)

        ev_good = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                             quantity=77, product_name='Good Widget')
        send_event(avro_producer_v1, ev_good)

        result = wait_for_inventory(cassandra_session, sku, zone, 77)
        assert result is not None, \
            f'Consumer did not recover: expected 77, got {get_inventory(cassandra_session, sku, zone)}'


class TestSchemaEvolution:
    """
    Проверяет, что V2-события (с supplier_id) корректно обрабатываются consumer.
    Путь: Kafka (V2 schema) → Consumer → Cassandra (supplier_id сохранён).
    """

    def test_v2_event_with_supplier_id_persisted(
        self, avro_producer_v2, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                        quantity=60, product_name='V2 Widget',
                        supplier_id='SUP-TEST-001')
        send_event(avro_producer_v2, ev)

        wait_for_inventory(cassandra_session, sku, zone, 60)

        row = cassandra_session.execute(
            'SELECT supplier_id FROM inventory_by_product_zone '
            'WHERE sku=%s AND zone_id=%s', (sku, zone)
        ).one()
        assert row is not None
        assert row.supplier_id == 'SUP-TEST-001', \
            f'Expected supplier_id=SUP-TEST-001, got {row.supplier_id}'


class TestConsumerObservability:
    """
    Проверяет, что consumer экспортирует метрики после обработки событий.
    Путь: Kafka → Consumer → /metrics endpoint.
    """

    def test_health_endpoint_available(self):
        resp = requests.get(f'{CONSUMER_URL}/health', timeout=5)
        assert resp.status_code == 200

    def test_metrics_endpoint_prometheus_format(self):
        resp = requests.get(f'{CONSUMER_URL}/metrics', timeout=5)
        assert resp.status_code == 200
        assert '# HELP' in resp.text or '# TYPE' in resp.text

    def test_events_processed_counter_increments(
        self, avro_producer_v1, cassandra_session, request
    ):
        sku = f'IT-{uuid.uuid4().hex[:8]}'
        zone = f'Z-{uuid.uuid4().hex[:8]}'
        request.node._track(skus=[sku], zones=[zone])

        def get_counter():
            resp = requests.get(f'{CONSUMER_URL}/metrics', timeout=5)
            for line in resp.text.splitlines():
                if line.startswith('events_processed_total{') and 'PRODUCT_RECEIVED' in line:
                    try:
                        return float(line.split()[-1])
                    except (ValueError, IndexError):
                        pass
            return 0.0

        before = get_counter()

        ev = make_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                        quantity=1, product_name='Metrics Widget')
        send_event(avro_producer_v1, ev)
        wait_for_inventory(cassandra_session, sku, zone, 1)

        after = get_counter()
        assert after > before, \
            f'events_processed_total did not increase: before={before}, after={after}'

    def test_schema_registry_has_warehouse_schema(self):
        resp = requests.get(f'{SCHEMA_REGISTRY_URL}/subjects', timeout=5)
        assert resp.status_code == 200
        assert 'warehouse-events-value' in resp.json()

    def test_schema_registry_has_multiple_versions(self):
        resp = requests.get(
            f'{SCHEMA_REGISTRY_URL}/subjects/warehouse-events-value/versions',
            timeout=5,
        )
        assert resp.status_code == 200
        assert len(resp.json()) >= 2
