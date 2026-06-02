import json
import time
import uuid
import requests
import pytest

from tests.e2e.conftest import (
    publish,
    new_event,
    wait_inv,
    get_inv,
    poll_cassandra,
    read_dlq_message,
    get_metrics_text,
    get_counter_value,
    CONSUMER_URL,
    SCHEMA_REGISTRY_URL,
    EVENT_PROPAGATION_TIMEOUT,
)


class TestWarehouseReceiveAndShip:
    """
    Сценарий: поступление товара на склад и его отгрузка.

    Шаги:
      1. Отправить PRODUCT_RECEIVED → Kafka
      2. Проверить HTTP /health (consumer жив)
      3. Проверить HTTP /metrics (статус 200, Prometheus-формат)
      4. Проверить inventory_by_product_zone в Cassandra (available увеличился)
      5. Проверить inventory_by_product в Cassandra (total_available обновлён)
      6. Проверить inventory_by_zone в Cassandra (запись создана)
      7. Проверить event_log в Cassandra (событие записано)
      8. Отправить PRODUCT_SHIPPED → Kafka
      9. Проверить inventory_by_product_zone (available уменьшился)
      10. Проверить счётчик events_processed_total вырос
    """

    def test_receive_and_ship_full_scenario(
        self, producer_v1, cassandra_session
    ):
        sku = f'E2E-{uuid.uuid4().hex[:8]}'
        zone = f'ZE-{uuid.uuid4().hex[:8]}'
        ts_base = int(time.time() * 1000)

        try:
            resp = requests.get(f'{CONSUMER_URL}/health', timeout=5)
            assert resp.status_code == 200, \
                f'Consumer /health: expected 200, got {resp.status_code}'
            assert resp.text.strip() == 'OK', \
                f'Consumer /health body: expected "OK", got {resp.text!r}'

            metrics_before = get_metrics_text()
            assert '# HELP' in metrics_before or '# TYPE' in metrics_before, \
                '/metrics is not in Prometheus format'
            assert 'events_processed_total' in metrics_before
            assert 'event_processing_duration_seconds' in metrics_before
            assert 'cassandra_write_errors_total' in metrics_before
            assert 'consumer_lag' in metrics_before
            counter_before = get_counter_value(metrics_before, 'PRODUCT_RECEIVED')

            ev_recv = new_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                                quantity=120, product_name='E2E Widget',
                                timestamp=ts_base)
            publish(producer_v1, ev_recv)

            result = wait_inv(cassandra_session, sku, zone, avail=120, reserved=0)
            assert result is not None, \
                f'inventory_by_product_zone not updated: got {get_inv(cassandra_session, sku, zone)}'
            avail_pz, res_pz = result
            assert avail_pz == 120
            assert res_pz == 0

            def check_product():
                row = cassandra_session.execute(
                    'SELECT total_available, total_reserved FROM inventory_by_product '
                    'WHERE sku=%s', (sku,)
                ).one()
                if row and row.total_available == 120:
                    return row
                return None

            row_p = poll_cassandra(check_product)
            assert row_p is not None, 'inventory_by_product not updated'
            assert row_p.total_available == 120
            assert (row_p.total_reserved or 0) == 0

            def check_zone():
                row = cassandra_session.execute(
                    'SELECT available, reserved FROM inventory_by_zone '
                    'WHERE zone_id=%s AND sku=%s', (zone, sku)
                ).one()
                return row if row and row.available == 120 else None

            row_z = poll_cassandra(check_zone)
            assert row_z is not None, 'inventory_by_zone not updated'
            assert row_z.available == 120

            def check_event_log():
                rows = list(cassandra_session.execute(
                    'SELECT event_type, event_id FROM event_log WHERE sku=%s', (sku,)
                ))
                return rows if rows else None

            log_rows = poll_cassandra(check_event_log)
            assert log_rows is not None, 'event_log not populated'
            event_types_logged = [r.event_type for r in log_rows]
            assert 'PRODUCT_RECEIVED' in event_types_logged, \
                f'PRODUCT_RECEIVED not in event_log: {event_types_logged}'

            ev_ship = new_event('PRODUCT_SHIPPED', sku=sku, zone_id=zone,
                                quantity=45, timestamp=ts_base + 1000)
            publish(producer_v1, ev_ship)

            result = wait_inv(cassandra_session, sku, zone, avail=75, reserved=0)
            assert result is not None, \
                f'inventory after PRODUCT_SHIPPED: expected 75, got {get_inv(cassandra_session, sku, zone)}'
            avail_after, _ = result
            assert avail_after == 75

            time.sleep(2)
            metrics_after = get_metrics_text()
            counter_after = get_counter_value(metrics_after, 'PRODUCT_RECEIVED')
            assert counter_after > counter_before, \
                f'events_processed_total[PRODUCT_RECEIVED] did not increase: {counter_before} → {counter_after}'

        finally:
            cassandra_session.execute(
                'DELETE FROM inventory_by_product WHERE sku=%s', (sku,)
            )
            cassandra_session.execute(
                'DELETE FROM inventory_by_product_zone WHERE sku=%s', (sku,)
            )
            cassandra_session.execute(
                'DELETE FROM inventory_by_zone WHERE zone_id=%s', (zone,)
            )
            cassandra_session.execute(
                'DELETE FROM event_log WHERE sku=%s', (sku,)
            )


class TestWarehouseOrderLifecycle:
    """
    Сценарий: полный жизненный цикл заказа.

    Шаги:
      1. PRODUCT_RECEIVED × 2 SKU → Kafka
      2. Проверить остатки в Cassandra
      3. ORDER_CREATED (multi-SKU) → Kafka
      4. Проверить резервирование в inventory_by_product_zone
      5. Проверить запись заказа в orders (status=CREATED, items корректны)
      6. ORDER_COMPLETED → Kafka
      7. Проверить освобождение reserved в inventory_by_product_zone
      8. Проверить статус заказа в orders (status=COMPLETED)
    """

    def test_order_lifecycle_full_scenario(
        self, producer_v1, cassandra_session
    ):
        sku_a = f'E2E-A-{uuid.uuid4().hex[:8]}'
        sku_b = f'E2E-B-{uuid.uuid4().hex[:8]}'
        zone = f'ZE-{uuid.uuid4().hex[:8]}'
        order_id = str(uuid.uuid4())
        ts = int(time.time() * 1000)

        try:
            ev_a = new_event('PRODUCT_RECEIVED', sku=sku_a, zone_id=zone,
                             quantity=100, product_name='Widget A', timestamp=ts)
            ev_b = new_event('PRODUCT_RECEIVED', sku=sku_b, zone_id=zone,
                             quantity=80, product_name='Widget B', timestamp=ts + 100)
            publish(producer_v1, ev_a)
            publish(producer_v1, ev_b)

            r_a = wait_inv(cassandra_session, sku_a, zone, avail=100, reserved=0)
            r_b = wait_inv(cassandra_session, sku_b, zone, avail=80, reserved=0)
            assert r_a is not None, f'sku_a not received: {get_inv(cassandra_session, sku_a, zone)}'
            assert r_b is not None, f'sku_b not received: {get_inv(cassandra_session, sku_b, zone)}'

            items = json.dumps([
                {'sku': sku_a, 'zone_id': zone, 'quantity': 30},
                {'sku': sku_b, 'zone_id': zone, 'quantity': 20},
            ])
            ev_order = new_event('ORDER_CREATED', order_id=order_id,
                                 items=items, timestamp=ts + 1000)
            publish(producer_v1, ev_order)

            r_a_res = wait_inv(cassandra_session, sku_a, zone, avail=70, reserved=30)
            r_b_res = wait_inv(cassandra_session, sku_b, zone, avail=60, reserved=20)
            assert r_a_res is not None, \
                f'sku_a after ORDER_CREATED: expected (70,30), got {get_inv(cassandra_session, sku_a, zone)}'
            assert r_b_res is not None, \
                f'sku_b after ORDER_CREATED: expected (60,20), got {get_inv(cassandra_session, sku_b, zone)}'

            def check_order_created():
                row = cassandra_session.execute(
                    'SELECT status, items FROM orders WHERE order_id=%s', (order_id,)
                ).one()
                return row if row and row.status == 'CREATED' else None

            order_row = poll_cassandra(check_order_created)
            assert order_row is not None, \
                f'Order {order_id} not found in orders table with status=CREATED'
            assert order_row.status == 'CREATED'

            stored_items = json.loads(order_row.items)
            assert isinstance(stored_items, list), \
                f'orders.items is not a JSON list: {order_row.items!r}'
            assert len(stored_items) == 2, \
                f'Expected 2 items in order, got {len(stored_items)}'
            stored_skus = {item['sku'] for item in stored_items}
            assert sku_a in stored_skus, f'{sku_a} not in order items'
            assert sku_b in stored_skus, f'{sku_b} not in order items'

            ev_complete = new_event('ORDER_COMPLETED', order_id=order_id,
                                    timestamp=ts + 2000)
            publish(producer_v1, ev_complete)

            r_a_done = wait_inv(cassandra_session, sku_a, zone, avail=70, reserved=0)
            r_b_done = wait_inv(cassandra_session, sku_b, zone, avail=60, reserved=0)
            assert r_a_done is not None, \
                f'sku_a after ORDER_COMPLETED: expected reserved=0, got {get_inv(cassandra_session, sku_a, zone)}'
            assert r_b_done is not None, \
                f'sku_b after ORDER_COMPLETED: expected reserved=0, got {get_inv(cassandra_session, sku_b, zone)}'

            def check_order_completed():
                row = cassandra_session.execute(
                    'SELECT status FROM orders WHERE order_id=%s', (order_id,)
                ).one()
                return row if row and row.status == 'COMPLETED' else None

            order_done = poll_cassandra(check_order_completed)
            assert order_done is not None, \
                f'Order {order_id} status not updated to COMPLETED'
            assert order_done.status == 'COMPLETED'

        finally:
            for sku in (sku_a, sku_b):
                cassandra_session.execute(
                    'DELETE FROM inventory_by_product WHERE sku=%s', (sku,)
                )
                cassandra_session.execute(
                    'DELETE FROM inventory_by_product_zone WHERE sku=%s', (sku,)
                )
                cassandra_session.execute(
                    'DELETE FROM event_log WHERE sku=%s', (sku,)
                )
            cassandra_session.execute(
                'DELETE FROM inventory_by_zone WHERE zone_id=%s', (zone,)
            )
            cassandra_session.execute(
                'DELETE FROM orders WHERE order_id=%s', (order_id,)
            )


class TestWarehouseSchemaEvolution:
    """
    Сценарий: обратная совместимость схем V1 и V2.

    Шаги:
      1. Проверить Schema Registry HTTP API: статус 200, тип ответа JSON
      2. Проверить, что subject warehouse-events-value существует
      3. Проверить, что зарегистрировано >= 2 версий
      4. Проверить совместимость BACKWARD через HTTP API Schema Registry
      5. Отправить V1-событие → Kafka → проверить Cassandra (supplier_id=None)
      6. Отправить V2-событие с supplier_id → Kafka → проверить Cassandra (supplier_id сохранён)
    """

    def test_schema_evolution_v1_and_v2(
        self, producer_v1, producer_v2, cassandra_session
    ):
        sku_v1 = f'E2E-V1-{uuid.uuid4().hex[:8]}'
        sku_v2 = f'E2E-V2-{uuid.uuid4().hex[:8]}'
        zone = f'ZE-{uuid.uuid4().hex[:8]}'
        ts = int(time.time() * 1000)

        try:
            resp = requests.get(f'{SCHEMA_REGISTRY_URL}/subjects', timeout=5)
            assert resp.status_code == 200, \
                f'Schema Registry /subjects: expected 200, got {resp.status_code}'
            assert resp.headers.get('Content-Type', '').startswith('application/'), \
                f'Schema Registry response Content-Type unexpected: {resp.headers.get("Content-Type")}'
            subjects = resp.json()
            assert isinstance(subjects, list), \
                f'Schema Registry /subjects response is not a list: {subjects!r}'
            assert 'warehouse-events-value' in subjects, \
                f'warehouse-events-value not registered. Subjects: {subjects}'

            resp_versions = requests.get(
                f'{SCHEMA_REGISTRY_URL}/subjects/warehouse-events-value/versions',
                timeout=5,
            )
            assert resp_versions.status_code == 200
            versions = resp_versions.json()
            assert isinstance(versions, list)
            assert len(versions) >= 2, \
                f'Expected >= 2 schema versions, got {versions}'

            for v in versions:
                resp_v = requests.get(
                    f'{SCHEMA_REGISTRY_URL}/subjects/warehouse-events-value/versions/{v}',
                    timeout=5,
                )
                assert resp_v.status_code == 200, \
                    f'Version {v} not accessible: {resp_v.status_code}'
                schema_info = resp_v.json()
                assert 'id' in schema_info, f'Version {v} response missing "id": {schema_info}'
                assert 'schema' in schema_info, f'Version {v} response missing "schema": {schema_info}'
                assert isinstance(schema_info['id'], int), \
                    f'schema id is not int: {schema_info["id"]!r}'

            resp_compat = requests.get(
                f'{SCHEMA_REGISTRY_URL}/config/warehouse-events-value',
                timeout=5,
            )
            assert resp_compat.status_code == 200
            compat_body = resp_compat.json()
            assert 'compatibilityLevel' in compat_body, \
                f'No compatibilityLevel in response: {compat_body}'
            assert compat_body['compatibilityLevel'] == 'BACKWARD', \
                f'Expected BACKWARD compatibility, got {compat_body["compatibilityLevel"]}'

            ev_v1 = new_event('PRODUCT_RECEIVED', sku=sku_v1, zone_id=zone,
                              quantity=50, product_name='Widget V1', timestamp=ts)
            publish(producer_v1, ev_v1)

            result_v1 = wait_inv(cassandra_session, sku_v1, zone, avail=50)
            assert result_v1 is not None, \
                f'V1 event not processed: {get_inv(cassandra_session, sku_v1, zone)}'

            row_v1 = cassandra_session.execute(
                'SELECT available, reserved, supplier_id FROM inventory_by_product_zone '
                'WHERE sku=%s AND zone_id=%s', (sku_v1, zone)
            ).one()
            assert row_v1 is not None
            assert row_v1.available == 50
            assert row_v1.supplier_id is None, \
                f'V1 event should have supplier_id=None, got {row_v1.supplier_id!r}'

            ev_v2 = new_event('PRODUCT_RECEIVED', sku=sku_v2, zone_id=zone,
                              quantity=75, product_name='Widget V2',
                              supplier_id='SUP-E2E-001', timestamp=ts + 1000)
            publish(producer_v2, ev_v2)

            result_v2 = wait_inv(cassandra_session, sku_v2, zone, avail=75)
            assert result_v2 is not None, \
                f'V2 event not processed: {get_inv(cassandra_session, sku_v2, zone)}'

            row_v2 = cassandra_session.execute(
                'SELECT available, reserved, supplier_id FROM inventory_by_product_zone '
                'WHERE sku=%s AND zone_id=%s', (sku_v2, zone)
            ).one()
            assert row_v2 is not None
            assert row_v2.available == 75
            assert row_v2.supplier_id == 'SUP-E2E-001', \
                f'V2 event supplier_id: expected "SUP-E2E-001", got {row_v2.supplier_id!r}'

        finally:
            for sku in (sku_v1, sku_v2):
                cassandra_session.execute(
                    'DELETE FROM inventory_by_product WHERE sku=%s', (sku,)
                )
                cassandra_session.execute(
                    'DELETE FROM inventory_by_product_zone WHERE sku=%s', (sku,)
                )
                cassandra_session.execute(
                    'DELETE FROM event_log WHERE sku=%s', (sku,)
                )
            cassandra_session.execute(
                'DELETE FROM inventory_by_zone WHERE zone_id=%s', (zone,)
            )


class TestWarehouseDLQScenario:
    """
    Сценарий: невалидное событие попадает в DLQ, система продолжает работу.

    Шаги:
      1. Отправить невалидное событие (quantity < 0) → Kafka
      2. Проверить HTTP /health (consumer жив, статус 200, тело "OK")
      3. Проверить DLQ: сообщение содержит original_event, error_code, error_reason,
         failed_at, kafka_metadata (partition, offset)
      4. Проверить, что невалидное событие НЕ попало в Cassandra
      5. Отправить валидное событие → Kafka
      6. Проверить, что валидное событие обработано (Cassandra обновлена)
    """

    def test_invalid_event_to_dlq_and_recovery(
        self, producer_v1, cassandra_session
    ):
        sku = f'E2E-DLQ-{uuid.uuid4().hex[:8]}'
        zone = f'ZE-{uuid.uuid4().hex[:8]}'

        try:
            ev_bad = new_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                               quantity=-50, product_name='Bad Widget')
            bad_event_id = ev_bad['event_id']
            publish(producer_v1, ev_bad)

            time.sleep(3)

            resp = requests.get(f'{CONSUMER_URL}/health', timeout=5)
            assert resp.status_code == 200, \
                f'/health after invalid event: expected 200, got {resp.status_code}'
            assert resp.text.strip() == 'OK', \
                f'/health body after invalid event: expected "OK", got {resp.text!r}'

            dlq_msg = read_dlq_message(bad_event_id, timeout=20)
            assert dlq_msg is not None, \
                f'DLQ message for event_id={bad_event_id} not found'

            assert 'original_event' in dlq_msg, \
                f'DLQ message missing "original_event": {list(dlq_msg.keys())}'
            assert 'error_code' in dlq_msg, \
                f'DLQ message missing "error_code": {list(dlq_msg.keys())}'
            assert 'error_reason' in dlq_msg, \
                f'DLQ message missing "error_reason": {list(dlq_msg.keys())}'
            assert 'failed_at' in dlq_msg, \
                f'DLQ message missing "failed_at": {list(dlq_msg.keys())}'
            assert 'kafka_metadata' in dlq_msg, \
                f'DLQ message missing "kafka_metadata": {list(dlq_msg.keys())}'

            assert dlq_msg['error_code'] == 'VALIDATION_ERROR', \
                f'DLQ error_code: expected "VALIDATION_ERROR", got {dlq_msg["error_code"]!r}'
            assert isinstance(dlq_msg['error_reason'], str) and len(dlq_msg['error_reason']) > 0, \
                f'DLQ error_reason is empty or not a string: {dlq_msg["error_reason"]!r}'
            assert isinstance(dlq_msg['failed_at'], str) and len(dlq_msg['failed_at']) > 0, \
                f'DLQ failed_at is empty or not a string: {dlq_msg["failed_at"]!r}'

            km = dlq_msg['kafka_metadata']
            assert 'partition' in km, f'kafka_metadata missing "partition": {km}'
            assert 'offset' in km, f'kafka_metadata missing "offset": {km}'
            assert isinstance(km['partition'], int), \
                f'kafka_metadata.partition is not int: {km["partition"]!r}'
            assert isinstance(km['offset'], int), \
                f'kafka_metadata.offset is not int: {km["offset"]!r}'

            orig = dlq_msg['original_event']
            assert orig.get('event_id') == bad_event_id, \
                f'DLQ original_event.event_id mismatch: {orig.get("event_id")} != {bad_event_id}'

            avail, _ = get_inv(cassandra_session, sku, zone)
            assert avail is None or avail == 0, \
                f'Invalid event should not update Cassandra, got available={avail}'

            ev_good = new_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                                quantity=33, product_name='Good Widget')
            publish(producer_v1, ev_good)

            result = wait_inv(cassandra_session, sku, zone, avail=33)
            assert result is not None, \
                f'Consumer did not recover: expected available=33, got {get_inv(cassandra_session, sku, zone)}'

        finally:
            cassandra_session.execute(
                'DELETE FROM inventory_by_product WHERE sku=%s', (sku,)
            )
            cassandra_session.execute(
                'DELETE FROM inventory_by_product_zone WHERE sku=%s', (sku,)
            )
            cassandra_session.execute(
                'DELETE FROM inventory_by_zone WHERE zone_id=%s', (zone,)
            )
            cassandra_session.execute(
                'DELETE FROM event_log WHERE sku=%s', (sku,)
            )


class TestWarehouseStaleEventHandling:
    """
    Сценарий: устаревшее событие (out-of-order) игнорируется.

    Шаги:
      1. PRODUCT_RECEIVED qty=100 timestamp=T → Kafka
      2. Проверить available=100 в Cassandra
      3. PRODUCT_SHIPPED qty=30 timestamp=T+2000 → Kafka
      4. Проверить available=70 в Cassandra
      5. PRODUCT_RECEIVED qty=999 timestamp=T+1000 (stale: T+1000 < T+2000) → Kafka
      6. Проверить available остался=70 (устаревшее событие проигнорировано)
    """

    def test_stale_event_ignored(self, producer_v1, cassandra_session):
        sku = f'E2E-STALE-{uuid.uuid4().hex[:8]}'
        zone = f'ZE-{uuid.uuid4().hex[:8]}'
        ts = int(time.time() * 1000)

        try:
            ev_recv = new_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                                quantity=100, product_name='Widget', timestamp=ts)
            publish(producer_v1, ev_recv)
            wait_inv(cassandra_session, sku, zone, avail=100)

            ev_ship = new_event('PRODUCT_SHIPPED', sku=sku, zone_id=zone,
                                quantity=30, timestamp=ts + 2000)
            publish(producer_v1, ev_ship)
            wait_inv(cassandra_session, sku, zone, avail=70)

            ev_stale = new_event('PRODUCT_RECEIVED', sku=sku, zone_id=zone,
                                 quantity=999, product_name='Widget',
                                 timestamp=ts + 1000)
            publish(producer_v1, ev_stale)

            time.sleep(EVENT_PROPAGATION_TIMEOUT)

            avail, _ = get_inv(cassandra_session, sku, zone)
            assert avail == 70, \
                f'Stale event was applied: expected available=70, got {avail}'

        finally:
            cassandra_session.execute(
                'DELETE FROM inventory_by_product WHERE sku=%s', (sku,)
            )
            cassandra_session.execute(
                'DELETE FROM inventory_by_product_zone WHERE sku=%s', (sku,)
            )
            cassandra_session.execute(
                'DELETE FROM inventory_by_zone WHERE zone_id=%s', (zone,)
            )
            cassandra_session.execute(
                'DELETE FROM event_log WHERE sku=%s', (sku,)
            )
