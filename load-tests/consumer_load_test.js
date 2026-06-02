import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('error_rate');
const healthLatency = new Trend('health_latency', true);
const metricsLatency = new Trend('metrics_latency', true);

export const options = {
  scenarios: {
    constant_load: {
      executor: 'constant-vus',
      vus: 10,
      duration: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500', 'p(99)<1000'],
    error_rate: ['rate<0.01'],
    health_latency: ['p(95)<300'],
    metrics_latency: ['p(95)<500'],
  },
};

const BASE_URL = __ENV.CONSUMER_URL || 'http://localhost:8000';

export default function () {
  const healthRes = http.get(`${BASE_URL}/health`, {
    tags: { endpoint: 'health' },
  });
  healthLatency.add(healthRes.timings.duration);
  const healthOk = check(healthRes, {
    'health status 200': (r) => r.status === 200,
    'health response not empty': (r) => r.body && r.body.length > 0,
  });
  errorRate.add(!healthOk);

  sleep(0.5);

  const metricsRes = http.get(`${BASE_URL}/metrics`, {
    tags: { endpoint: 'metrics' },
  });
  metricsLatency.add(metricsRes.timings.duration);
  const metricsOk = check(metricsRes, {
    'metrics status 200': (r) => r.status === 200,
    'metrics contains events_processed_total': (r) =>
      r.body && r.body.includes('events_processed_total'),
    'metrics contains consumer_lag': (r) =>
      r.body && r.body.includes('consumer_lag'),
    'metrics prometheus format': (r) =>
      r.body && r.body.includes('# HELP'),
  });
  errorRate.add(!metricsOk);

  sleep(0.5);
}

export function handleSummary(data) {
  return {
    'stdout': textSummary(data, { indent: '  ', enableColors: false }),
    '/reports/load-test-summary.json': JSON.stringify(data, null, 2),
  };
}

function textSummary(data, opts) {
  const indent = (opts && opts.indent) || '';
  const lines = [];
  lines.push('');
  lines.push(`${indent}Load Test Summary`);
  lines.push(`${indent}=================`);

  const metrics = data.metrics;
  const dur = metrics['http_req_duration'];
  if (dur) {
    lines.push(`${indent}HTTP Request Duration:`);
    lines.push(`${indent}  avg=${fmt(dur.values.avg)}ms  p50=${fmt(dur.values['p(50)'])}ms  p95=${fmt(dur.values['p(95)'])}ms  p99=${fmt(dur.values['p(99)'])}ms`);
  }

  const failed = metrics['http_req_failed'];
  if (failed) {
    lines.push(`${indent}HTTP Failures: ${pct(failed.values.rate)}`);
  }

  const reqs = metrics['http_reqs'];
  if (reqs) {
    lines.push(`${indent}Total Requests: ${reqs.values.count}  Rate: ${fmt(reqs.values.rate)}/s`);
  }

  const errRate = metrics['error_rate'];
  if (errRate) {
    lines.push(`${indent}Check Error Rate: ${pct(errRate.values.rate)}`);
  }

  const hl = metrics['health_latency'];
  if (hl) {
    lines.push(`${indent}Health Endpoint p95: ${fmt(hl.values['p(95)'])}ms`);
  }

  const ml = metrics['metrics_latency'];
  if (ml) {
    lines.push(`${indent}Metrics Endpoint p95: ${fmt(ml.values['p(95)'])}ms`);
  }

  lines.push('');
  return lines.join('\n');
}

function fmt(v) {
  return v !== undefined ? v.toFixed(2) : 'N/A';
}

function pct(v) {
  return v !== undefined ? (v * 100).toFixed(2) + '%' : 'N/A';
}
