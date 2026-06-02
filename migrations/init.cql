CREATE KEYSPACE IF NOT EXISTS warehouse
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'dc1': 3
    };

USE warehouse;

CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    sku                  TEXT,
    zone_id              TEXT,
    available            INT,
    reserved             INT,
    updated_at           TIMESTAMP,
    last_event_timestamp BIGINT,
    supplier_id          TEXT,
    PRIMARY KEY (sku, zone_id)
);

CREATE TABLE IF NOT EXISTS inventory_by_product (
    sku             TEXT PRIMARY KEY,
    total_available INT,
    total_reserved  INT,
    updated_at      TIMESTAMP,
    supplier_id     TEXT
);

CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id              TEXT,
    sku                  TEXT,
    available            INT,
    reserved             INT,
    updated_at           TIMESTAMP,
    last_event_timestamp BIGINT,
    supplier_id          TEXT,
    PRIMARY KEY (zone_id, sku)
);

CREATE TABLE IF NOT EXISTS event_log (
    sku          TEXT,
    occurred_at  TIMESTAMP,
    event_id     TEXT,
    event_type   TEXT,
    zone_id      TEXT,
    quantity     INT,
    order_id     TEXT,
    PRIMARY KEY (sku, occurred_at, event_id)
) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id ASC);

CREATE TABLE IF NOT EXISTS orders (
    order_id   TEXT PRIMARY KEY,
    status     TEXT,
    items      TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT,
    processed_at TIMESTAMP
);
