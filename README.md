# Часть 1

```bash
docker compose -f docker-compose.test.yml up --exit-code-from integration-tests
```

# Часть 2

```bash
./run_integration_tests.sh
```

# Часть 3

```bash
./run_e2e_tests.sh
```

# Часть 7

```bash
mkdir -p reports1
k6 run --env CONSUMER_URL=http://localhost:8000 \
  --out json=reports1/load-test-results.json \
  load-tests/consumer_load_test.js
```

# Часть 9

## Проверка срабатывания алёртов

```bash
docker compose up -d
```
```bash
docker compose stop consumer
```

Через 1 минуту в Prometheus UI (http://localhost:9090/alerts) алерт ConsumerDown перейдёт в firing

В Alertmanager UI (http://localhost:9093) алерт появится в разделе Alerts

В Grafana (http://localhost:3000) алерты видны через встроенный Alertmanager datasource

# Часть 10 — SLI и пороги отказа

## Определение SLI

Система Smart Warehouse состоит из двух компонентов: **producer** (публикует события в Kafka) и **consumer** (читает события из Kafka, записывает в Cassandra, экспортирует метрики на порту 8000). Определены три SLI для всей системы.

---

### SLI 1 — API Availability (Доступность HTTP API)

**Что измеряется:** доля успешных HTTP-запросов к consumer из всех запросов за скользящее окно 5 минут. Включает запросы к `/health` и `/metrics`.

**PromQL:**
```promql
(
  sum(rate(http_requests_total{job="warehouse-consumer"}[5m]))
  -
  sum(rate(http_request_errors_total{job="warehouse-consumer"}[5m]))
)
/
(sum(rate(http_requests_total{job="warehouse-consumer"}[5m])) + 0.001)
```

| Уровень | Значение | Обоснование |
|---------|----------|-------------|
| SLO (норма) | ≥ 99.5% | Стандарт для внутренних складских сервисов; допускает не более 0.5% ошибок |
| Порог отказа | < 95% | При 5%+ ошибок сервис деградирован настолько, что мониторинг и health-check ненадёжны |

**Алерты:** `SLI_ApiAvailabilityWarning` (< 99.5%, warning), `SLI_ApiAvailabilityBreached` (< 95%, critical)

**Использование в CI:** проверка `SLI1_api_availability` в [`load-tests/check_prometheus_metrics.sh`](load-tests/check_prometheus_metrics.sh) — при значении < 0.95 CI падает с exit code 1.

---

### SLI 2 — Event Processing Latency p95 (Задержка обработки событий)

**Что измеряется:** 95-й перцентиль времени от получения события из Kafka до завершения записи в Cassandra (метрика `event_processing_duration_seconds`).

**PromQL:**
```promql
histogram_quantile(0.95,
  sum(rate(event_processing_duration_seconds_bucket{job="warehouse-consumer"}[5m])) by (le)
)
```

| Уровень | Значение | Обоснование |
|---------|----------|-------------|
| SLO (норма) | < 2000ms | Kafka poll (≤500ms) + Cassandra write (≤500ms) + overhead; 2s — реалистичный бюджет для одной ноды |
| Порог отказа | > 5000ms | При 5s+ задержке consumer отстаёт от producer, lag растёт экспоненциально |

**Алерты:** `SLI_EventProcessingLatencyWarning` (> 2s, warning), `SLI_EventProcessingLatencyBreached` (> 5s, critical)

**Использование в CI:** проверка `SLI2_event_processing_p95_ms` в [`load-tests/check_prometheus_metrics.sh`](load-tests/check_prometheus_metrics.sh) — при значении > 5000ms CI падает.

---

### SLI 3 — Consumer Lag (Отставание потребителя)

**Что измеряется:** суммарное количество необработанных сообщений в Kafka-топике `warehouse-events` по всем партициям.

**PromQL:**
```promql
sum(consumer_lag{job="warehouse-consumer"})
```

| Уровень | Значение | Обоснование |
|---------|----------|-------------|
| SLO (норма) | < 100 сообщений | При 3 партициях и нормальной нагрузке lag должен быть близок к нулю; 100 — буфер на кратковременные всплески |
| Порог отказа | > 500 сообщений | При 500+ сообщениях consumer не успевает обрабатывать входящий поток; данные в Cassandra устаревают |

**Алерты:** `SLI_ConsumerLagWarning` (> 100, warning), `SLI_ConsumerLagBreached` (> 500, critical)

**Использование в CI:** проверка `SLI3_consumer_lag` в [`load-tests/check_prometheus_metrics.sh`](load-tests/check_prometheus_metrics.sh) — при значении > 500 CI падает.

---

## Где используются SLI

| SLI | Алерт в alerts.yml | Проверка в CI (check_prometheus_metrics.sh) |
|-----|--------------------|---------------------------------------------|
| API Availability | `SLI_ApiAvailabilityWarning`, `SLI_ApiAvailabilityBreached` | `SLI1_api_availability >= 0.95` |
| Event Processing Latency p95 | `SLI_EventProcessingLatencyWarning`, `SLI_EventProcessingLatencyBreached` | `SLI2_event_processing_p95_ms < 5000` |
| Consumer Lag | `SLI_ConsumerLagWarning`, `SLI_ConsumerLagBreached` | `SLI3_consumer_lag < 500` |

Все три SLI проверяются в CI job `e2e-load-metrics` — при нарушении порога отказа job завершается с exit code 1 и pipeline падает.
