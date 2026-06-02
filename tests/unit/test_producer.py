import sys
import os
import time
import json
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../producer'))

sys.modules['confluent_kafka'] = MagicMock()
sys.modules['confluent_kafka.schema_registry'] = MagicMock()
sys.modules['confluent_kafka.schema_registry.avro'] = MagicMock()
sys.modules['confluent_kafka.serialization'] = MagicMock()

import producer as p


class TestMakeEvent:
    def test_base_fields_present(self):
        ev = p.make_event('PRODUCT_RECEIVED')
        assert 'event_id' in ev
        assert 'event_type' in ev
        assert 'timestamp' in ev
        assert ev['event_type'] == 'PRODUCT_RECEIVED'

    def test_event_id_is_uuid_string(self):
        import uuid
        ev = p.make_event('PRODUCT_RECEIVED')
        uuid.UUID(ev['event_id'])

    def test_timestamp_is_milliseconds(self):
        before = int(time.time() * 1000)
        ev = p.make_event('PRODUCT_RECEIVED')
        after = int(time.time() * 1000)
        assert before <= ev['timestamp'] <= after

    def test_product_received_fields(self):
        ev = p.make_event('PRODUCT_RECEIVED')
        assert ev['sku'] in p.SKUS
        assert ev['zone_id'] in p.ZONES
        assert ev['quantity'] is not None
        assert 1 <= ev['quantity'] <= 50
        assert ev['product_name'] == p.PRODUCT_NAMES[ev['sku']]

    def test_product_shipped_fields(self):
        ev = p.make_event('PRODUCT_SHIPPED')
        assert ev['sku'] in p.SKUS
        assert ev['zone_id'] in p.ZONES
        assert ev['quantity'] is not None

    def test_product_moved_fields(self):
        ev = p.make_event('PRODUCT_MOVED')
        assert ev['sku'] in p.SKUS
        assert ev['source_zone_id'] in p.ZONES
        assert ev['destination_zone_id'] in p.ZONES
        assert ev['source_zone_id'] != ev['destination_zone_id']

    def test_product_reserved_has_order_id(self):
        ev = p.make_event('PRODUCT_RESERVED')
        assert ev['order_id'] is not None

    def test_product_released_has_order_id(self):
        ev = p.make_event('PRODUCT_RELEASED')
        assert ev['order_id'] is not None

    def test_inventory_counted_fields(self):
        ev = p.make_event('INVENTORY_COUNTED')
        assert ev['sku'] in p.SKUS
        assert ev['zone_id'] in p.ZONES
        assert ev['quantity'] is not None

    def test_order_created_has_items(self):
        ev = p.make_event('ORDER_CREATED')
        assert ev['order_id'] is not None
        assert ev['items'] is not None
        items = json.loads(ev['items'])
        assert isinstance(items, list)
        assert len(items) >= 1

    def test_order_created_items_structure(self):
        ev = p.make_event('ORDER_CREATED')
        items = json.loads(ev['items'])
        for item in items:
            assert 'sku' in item
            assert 'zone_id' in item
            assert 'quantity' in item
            assert item['sku'] in p.SKUS
            assert item['zone_id'] in p.ZONES

    def test_order_completed_has_order_id(self):
        ev = p.make_event('ORDER_COMPLETED')
        assert ev['order_id'] is not None

    def test_v2_event_has_supplier_id(self):
        ev = p.make_event('PRODUCT_RECEIVED', use_v2=True)
        assert 'supplier_id' in ev
        assert ev['supplier_id'] in p.SUPPLIERS

    def test_v1_event_has_no_supplier_id(self):
        ev = p.make_event('PRODUCT_RECEIVED', use_v2=False)
        assert 'supplier_id' not in ev

    def test_unknown_event_type_returns_base(self):
        ev = p.make_event('UNKNOWN_TYPE')
        assert ev['event_type'] == 'UNKNOWN_TYPE'
        assert ev['sku'] is None
        assert ev['zone_id'] is None


class TestDeliveryReport:
    def test_logs_error_on_failure(self):
        msg = MagicMock()
        msg.key.return_value = 'test-key'
        with patch.object(p.logger, 'error') as mock_log:
            p.delivery_report('some error', msg)
            mock_log.assert_called_once()

    def test_logs_info_on_success(self):
        msg = MagicMock()
        msg.topic.return_value = 'warehouse-events'
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        with patch.object(p.logger, 'info') as mock_log:
            p.delivery_report(None, msg)
            mock_log.assert_called_once()


class TestConstants:
    def test_skus_not_empty(self):
        assert len(p.SKUS) > 0

    def test_zones_not_empty(self):
        assert len(p.ZONES) > 0

    def test_suppliers_not_empty(self):
        assert len(p.SUPPLIERS) > 0

    def test_product_names_covers_all_skus(self):
        for sku in p.SKUS:
            assert sku in p.PRODUCT_NAMES, f'SKU {sku} not in PRODUCT_NAMES'

    def test_zones_have_at_least_two_for_move(self):
        assert len(p.ZONES) >= 2

    def test_order_created_items_count_range(self):
        for _ in range(20):
            ev = p.make_event('ORDER_CREATED')
            items = json.loads(ev['items'])
            assert 1 <= len(items) <= 3
