import json
import time
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../consumer'))

import consumer as c


def make_session():
    session = MagicMock()
    session.execute.return_value.one.return_value = None
    return session


def base_event(event_type, **kwargs):
    ev = {
        'event_id': 'test-event-id',
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
        'supplier_id': None,
    }
    ev.update(kwargs)
    return ev


class TestValidateEvent:
    def test_valid_product_received(self):
        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A', quantity=10)
        c.validate_event(ev)

    def test_negative_quantity_raises(self):
        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A', quantity=-1)
        with pytest.raises(c.ValidationError) as exc_info:
            c.validate_event(ev)
        assert exc_info.value.code == 'VALIDATION_ERROR'

    def test_missing_sku_raises(self):
        ev = base_event('PRODUCT_RECEIVED', sku=None, zone_id='ZONE-A', quantity=10)
        with pytest.raises(c.ValidationError):
            c.validate_event(ev)

    def test_missing_zone_id_raises(self):
        ev = base_event('PRODUCT_SHIPPED', sku='SKU-001', zone_id=None, quantity=5)
        with pytest.raises(c.ValidationError):
            c.validate_event(ev)

    def test_product_moved_missing_source_zone(self):
        ev = base_event('PRODUCT_MOVED', sku='SKU-001',
                        source_zone_id=None, destination_zone_id='ZONE-B', quantity=5)
        with pytest.raises(c.ValidationError):
            c.validate_event(ev)

    def test_product_moved_missing_destination_zone(self):
        ev = base_event('PRODUCT_MOVED', sku='SKU-001',
                        source_zone_id='ZONE-A', destination_zone_id=None, quantity=5)
        with pytest.raises(c.ValidationError):
            c.validate_event(ev)

    def test_product_moved_valid(self):
        ev = base_event('PRODUCT_MOVED', sku='SKU-001',
                        source_zone_id='ZONE-A', destination_zone_id='ZONE-B', quantity=5)
        c.validate_event(ev)

    def test_order_created_no_sku_required(self):
        ev = base_event('ORDER_CREATED', order_id='ORD-001', items='[]')
        c.validate_event(ev)

    def test_zero_quantity_is_valid(self):
        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A', quantity=0)
        c.validate_event(ev)

    def test_none_quantity_is_valid(self):
        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A', quantity=None)
        c.validate_event(ev)


class TestIsStale:
    def test_stale_event(self):
        assert c.is_stale(100, 200) is True

    def test_equal_timestamp_is_stale(self):
        assert c.is_stale(100, 100) is True

    def test_fresh_event(self):
        assert c.is_stale(200, 100) is False

    def test_zero_last_ts(self):
        assert c.is_stale(1, 0) is False


class TestGetInventory:
    def test_returns_zeros_when_no_row(self):
        session = make_session()
        session.execute.return_value.one.return_value = None
        avail, res, last_ts = c.get_inventory(session, 'SKU-001', 'ZONE-A')
        assert avail == 0
        assert res == 0
        assert last_ts == 0

    def test_returns_values_from_row(self):
        session = make_session()
        row = MagicMock()
        row.available = 50
        row.reserved = 10
        row.last_event_timestamp = 999
        session.execute.return_value.one.return_value = row
        avail, res, last_ts = c.get_inventory(session, 'SKU-001', 'ZONE-A')
        assert avail == 50
        assert res == 10
        assert last_ts == 999

    def test_handles_none_values_in_row(self):
        session = make_session()
        row = MagicMock()
        row.available = None
        row.reserved = None
        row.last_event_timestamp = None
        session.execute.return_value.one.return_value = row
        avail, res, last_ts = c.get_inventory(session, 'SKU-001', 'ZONE-A')
        assert avail == 0
        assert res == 0
        assert last_ts == 0


class TestHandleProductReceived:
    def test_increases_available(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        row = MagicMock()
        row.available = 50
        row.reserved = 0
        row.last_event_timestamp = ts - 1000
        session.execute.return_value.one.return_value = row
        session.execute.return_value.__iter__ = MagicMock(return_value=iter([]))

        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A',
                        quantity=30, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_product_received(session, ev, now)
            mock_write.assert_called_once()
            args = mock_write.call_args[0]
            assert args[3] == 100
            assert args[4] == 0

    def test_stale_event_skipped(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        row = MagicMock()
        row.available = 50
        row.reserved = 0
        row.last_event_timestamp = ts + 5000
        session.execute.return_value.one.return_value = row

        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A',
                        quantity=30, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_product_received(session, ev, now)
            mock_write.assert_not_called()


class TestHandleProductShipped:
    def test_decreases_available(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        row = MagicMock()
        row.available = 100
        row.reserved = 0
        row.last_event_timestamp = ts - 1000
        session.execute.return_value.one.return_value = row
        session.execute.return_value.__iter__ = MagicMock(return_value=iter([]))

        ev = base_event('PRODUCT_SHIPPED', sku='SKU-001', zone_id='ZONE-A',
                        quantity=20, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_product_shipped(session, ev, now)
            mock_write.assert_called_once()
            args = mock_write.call_args[0]
            assert args[3] == 80


class TestHandleProductReserved:
    def test_moves_from_available_to_reserved(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        row = MagicMock()
        row.available = 100
        row.reserved = 10
        row.last_event_timestamp = ts - 1000
        session.execute.return_value.one.return_value = row
        session.execute.return_value.__iter__ = MagicMock(return_value=iter([]))

        ev = base_event('PRODUCT_RESERVED', sku='SKU-001', zone_id='ZONE-A',
                        quantity=30, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_product_reserved(session, ev, now)
            mock_write.assert_called_once()
            args = mock_write.call_args[0]
            assert args[3] == 70
            assert args[4] == 40


class TestHandleProductReleased:
    def test_moves_from_reserved_to_available(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        row = MagicMock()
        row.available = 70
        row.reserved = 30
        row.last_event_timestamp = ts - 1000
        session.execute.return_value.one.return_value = row
        session.execute.return_value.__iter__ = MagicMock(return_value=iter([]))

        ev = base_event('PRODUCT_RELEASED', sku='SKU-001', zone_id='ZONE-A',
                        quantity=30, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_product_released(session, ev, now)
            mock_write.assert_called_once()
            args = mock_write.call_args[0]
            assert args[3] == 100
            assert args[4] == 0


class TestHandleInventoryCounted:
    def test_sets_absolute_quantity(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        row = MagicMock()
        row.available = 999
        row.reserved = 999
        row.last_event_timestamp = ts - 1000
        session.execute.return_value.one.return_value = row
        session.execute.return_value.__iter__ = MagicMock(return_value=iter([]))

        ev = base_event('INVENTORY_COUNTED', sku='SKU-001', zone_id='ZONE-A',
                        quantity=42, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_inventory_counted(session, ev, now)
            mock_write.assert_called_once()
            args = mock_write.call_args[0]
            assert args[3] == 42
            assert args[4] == 0


class TestHandleOrderCreated:
    def test_creates_order_and_reserves_items(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        inv_row = MagicMock()
        inv_row.available = 100
        inv_row.reserved = 0
        inv_row.last_event_timestamp = ts - 1000

        session.execute.return_value.one.return_value = inv_row
        session.execute.return_value.__iter__ = MagicMock(return_value=iter([]))

        items = json.dumps([{'sku': 'SKU-001', 'zone_id': 'ZONE-A', 'quantity': 10}])
        ev = base_event('ORDER_CREATED', order_id='ORD-001', items=items, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_order_created(session, ev, now)
            assert session.execute.called
            mock_write.assert_called_once()

    def test_handles_empty_items(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ev = base_event('ORDER_CREATED', order_id='ORD-002', items='[]')

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_order_created(session, ev, now)
            mock_write.assert_not_called()


class TestHandleProductMoved:
    def test_moves_between_zones(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ts = int(time.time() * 1000)

        row = MagicMock()
        row.available = 100
        row.reserved = 0
        row.last_event_timestamp = ts - 1000
        session.execute.return_value.one.return_value = row
        session.execute.return_value.__iter__ = MagicMock(return_value=iter([]))

        ev = base_event('PRODUCT_MOVED', sku='SKU-001',
                        source_zone_id='ZONE-A', destination_zone_id='ZONE-B',
                        quantity=25, timestamp=ts)

        with patch.object(c, 'write_inventory') as mock_write:
            c.handle_product_moved(session, ev, now)
            assert mock_write.call_count == 2
            src_args = mock_write.call_args_list[0][0]
            assert src_args[3] == 75
            dst_args = mock_write.call_args_list[1][0]
            assert dst_args[3] == 125


class TestSendToDlq:
    def test_sends_json_to_dlq(self):
        dlq_producer = MagicMock()
        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A', quantity=-1)
        exc = c.ValidationError('bad quantity', 'VALIDATION_ERROR')

        c.send_to_dlq(dlq_producer, 'warehouse-events-dlq', ev, exc, partition=0, offset=42)

        dlq_producer.produce.assert_called_once()
        call_kwargs = dlq_producer.produce.call_args
        raw = call_kwargs[1]['value'] if call_kwargs[1] else call_kwargs[0][1]
        msg = json.loads(raw.decode('utf-8'))
        assert msg['error_code'] == 'VALIDATION_ERROR'
        assert msg['kafka_metadata']['partition'] == 0
        assert msg['kafka_metadata']['offset'] == 42
        dlq_producer.flush.assert_called_once()


class TestValidationError:
    def test_default_code(self):
        err = c.ValidationError('some error')
        assert err.code == 'VALIDATION_ERROR'
        assert str(err) == 'some error'

    def test_custom_code(self):
        err = c.ValidationError('msg', 'CUSTOM_CODE')
        assert err.code == 'CUSTOM_CODE'


class TestAppendEventLog:
    def test_skips_event_without_sku(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ev = base_event('ORDER_COMPLETED', order_id='ORD-001')
        c.append_event_log(session, ev, now)
        session.execute.assert_not_called()

    def test_inserts_event_with_sku(self):
        session = make_session()
        now = datetime.now(timezone.utc)
        ev = base_event('PRODUCT_RECEIVED', sku='SKU-001', zone_id='ZONE-A', quantity=10)
        c.append_event_log(session, ev, now)
        session.execute.assert_called_once()
