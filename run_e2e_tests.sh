#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose.test.yml"

cleanup() {
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

$COMPOSE down -v --remove-orphans 2>/dev/null || true

echo "Starting infrastructure and waiting for healthchecks..."
$COMPOSE up -d --wait zookeeper kafka schema-registry cassandra

echo "Initializing Kafka topics..."
$COMPOSE up --no-log-prefix --exit-code-from kafka-init kafka-init

echo "Initializing Cassandra schema..."
$COMPOSE up --no-log-prefix --exit-code-from cassandra-init cassandra-init

echo "Registering schemas in Schema Registry..."
$COMPOSE up --no-log-prefix --exit-code-from schema-init schema-init

echo "Starting consumer and waiting for it to become healthy..."
$COMPOSE up -d --wait consumer

mkdir -p reports

echo "Running E2E tests..."
$COMPOSE run --rm \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
    -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    -e CASSANDRA_HOSTS=cassandra \
    -e CASSANDRA_PORT=9042 \
    -e CASSANDRA_KEYSPACE=warehouse \
    -e CONSUMER_URL=http://consumer:8000 \
    -e KAFKA_TOPIC=warehouse-events \
    -e KAFKA_DLQ_TOPIC=warehouse-events-dlq \
    -e EVENT_PROPAGATION_TIMEOUT=15 \
    -v "$(pwd)/reports:/reports" \
    e2e-tests
