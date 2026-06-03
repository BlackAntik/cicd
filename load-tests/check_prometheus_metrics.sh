#!/usr/bin/env bash
set -euo pipefail

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
REPORT_FILE="${REPORT_FILE:-/tmp/prometheus-check-report.json}"

PASS=0
FAIL=0
RESULTS=()

query() {
  local expr="$1"
  curl -sf "${PROMETHEUS_URL}/api/v1/query" \
    --data-urlencode "query=${expr}" \
    | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
if results:
    print(results[0]['value'][1])
else:
    print('NaN')
"
}

json_str() {
  python3 -c "import json, sys; print(json.dumps(sys.argv[1]))" "$1"
}

check() {
  local name="$1"
  local expr="$2"
  local op="$3"
  local threshold="$4"
  local rationale="$5"

  local value
  value=$(query "$expr" 2>/dev/null || echo "NaN")

  local status="PASS"
  local passed=true

  if [ "$value" = "NaN" ] || [ -z "$value" ]; then
    status="WARN"
    passed=false
    echo "[WARN] ${name}: no data (expr: ${expr})"
  else
    local cmp
    cmp=$(python3 -c "
v = float('${value}')
t = float('${threshold}')
ops = {'<': v < t, '<=': v <= t, '>': v > t, '>=': v >= t, '==': v == t}
print('true' if ops.get('${op}', False) else 'false')
")
    if [ "$cmp" = "true" ]; then
      echo "[PASS] ${name}: ${value} ${op} ${threshold} — ${rationale}"
    else
      status="FAIL"
      passed=false
      echo "[FAIL] ${name}: ${value} NOT ${op} ${threshold} — ${rationale}"
    fi
  fi

  if [ "$passed" = "true" ]; then
    PASS=$((PASS + 1))
  else
    if [ "$status" = "FAIL" ]; then
      FAIL=$((FAIL + 1))
    fi
  fi

  RESULTS+=("{\"check\":$(json_str "$name"),\"expr\":$(json_str "$expr"),\"value\":$(json_str "$value"),\"op\":$(json_str "$op"),\"threshold\":$(json_str "$threshold"),\"status\":$(json_str "$status"),\"rationale\":$(json_str "$rationale")}")
}

echo "=== Prometheus Metrics Check ==="
echo "Prometheus: ${PROMETHEUS_URL}"
echo ""

check \
  "consumer_http_error_rate" \
  "sum(rate(http_request_errors_total{job=\"warehouse-consumer\"}[2m])) / (sum(rate(http_requests_total{job=\"warehouse-consumer\"}[2m])) + 0.001)" \
  "<" \
  "0.01" \
  "HTTP error rate must be below 1%"

check \
  "consumer_http_p95_latency_ms" \
  "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job=\"warehouse-consumer\"}[2m])) by (le)) * 1000" \
  "<" \
  "500" \
  "p95 HTTP latency must be below 500ms"

check \
  "consumer_http_p99_latency_ms" \
  "histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{job=\"warehouse-consumer\"}[2m])) by (le)) * 1000" \
  "<" \
  "1000" \
  "p99 HTTP latency must be below 1000ms"

check \
  "consumer_event_processing_p95_ms" \
  "histogram_quantile(0.95, sum(rate(event_processing_duration_seconds_bucket{job=\"warehouse-consumer\"}[2m])) by (le)) * 1000" \
  "<" \
  "2000" \
  "p95 event processing latency must be below 2000ms (Kafka+Cassandra round-trip)"

check \
  "consumer_cassandra_error_rate" \
  "sum(rate(cassandra_write_errors_total{job=\"warehouse-consumer\"}[2m])) / (sum(rate(events_processed_total{job=\"warehouse-consumer\"}[2m])) + 0.001)" \
  "<" \
  "0.05" \
  "Cassandra write error rate must be below 5%"

check \
  "consumer_events_processed_total" \
  "sum(events_processed_total{job=\"warehouse-consumer\"})" \
  ">" \
  "0" \
  "At least one event must have been processed during the test"

check \
  "consumer_http_rps" \
  "sum(rate(http_requests_total{job=\"warehouse-consumer\"}[2m]))" \
  ">" \
  "0" \
  "Consumer must be receiving HTTP requests during load test"

check \
  "consumer_lag_not_growing" \
  "sum(consumer_lag{job=\"warehouse-consumer\"})" \
  "<" \
  "1000" \
  "Consumer lag must stay below 1000 messages (not falling behind)"

echo ""
echo "=== SLI Checks ==="

check \
  "SLI1_api_availability" \
  "(sum(rate(http_requests_total{job=\"warehouse-consumer\"}[2m])) - sum(rate(http_request_errors_total{job=\"warehouse-consumer\"}[2m]))) / (sum(rate(http_requests_total{job=\"warehouse-consumer\"}[2m])) + 0.001)" \
  ">=" \
  "0.95" \
  "SLI1 API Availability: failure threshold=95% (SLO=99.5%)"

check \
  "SLI2_event_processing_p95_ms" \
  "histogram_quantile(0.95, sum(rate(event_processing_duration_seconds_bucket{job=\"warehouse-consumer\"}[2m])) by (le)) * 1000" \
  "<" \
  "5000" \
  "SLI2 Event Processing Latency p95: failure threshold=5000ms (SLO=2000ms)"

check \
  "SLI3_consumer_lag" \
  "sum(consumer_lag{job=\"warehouse-consumer\"})" \
  "<" \
  "500" \
  "SLI3 Consumer Lag: failure threshold=500 messages (SLO=100 messages)"

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="

RESULTS_JSON=$(IFS=,; echo "[${RESULTS[*]}]")
python3 -c "
import json, sys
results = json.loads(sys.argv[1])
report = {
    'passed': ${PASS},
    'failed': ${FAIL},
    'total': ${PASS} + ${FAIL},
    'checks': results
}
print(json.dumps(report, indent=2))
" "$RESULTS_JSON" > "${REPORT_FILE}"

echo "Report saved to ${REPORT_FILE}"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "ERROR: ${FAIL} check(s) failed. CI pipeline will fail."
  exit 1
fi

echo "All checks passed."
