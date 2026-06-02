#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose.test.yml"

cleanup() {
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

$COMPOSE down -v --remove-orphans 2>/dev/null || true

$COMPOSE up -d zookeeper kafka schema-registry cassandra

echo "Waiting for Zookeeper..."
timeout 60 bash -c \
    'until docker compose -f docker-compose.test.yml exec -T zookeeper \
        bash -c "echo ruok | nc localhost 2181" 2>/dev/null | grep -q imok; \
    do sleep 2; done'

echo "Waiting for Kafka..."
timeout 120 bash -c \
    'until docker compose -f docker-compose.test.yml exec -T kafka \
        kafka-broker-api-versions --bootstrap-server localhost:9092 \
        >/dev/null 2>&1; \
    do sleep 5; done'

echo "Waiting for Schema Registry..."
timeout 120 bash -c \
    'until curl -sf http://localhost:8081/subjects >/dev/null 2>&1; \
    do sleep 5; done'

echo "Waiting for Cassandra..."
timeout 240 bash -c \
    'until docker compose -f docker-compose.test.yml exec -T cassandra \
        nodetool status 2>/dev/null | grep -qE "^UN\s+"; \
    do sleep 10; done'

echo "Initializing Kafka topics, Cassandra schema and Schema Registry..."
$COMPOSE up kafka-init cassandra-init schema-init --exit-code-from schema-init

echo "Starting consumer..."
$COMPOSE up -d consumer

echo "Waiting for consumer /health..."
timeout 120 bash -c \
    'until curl -sf http://localhost:8000/health >/dev/null 2>&1; \
    do sleep 5; done'

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
