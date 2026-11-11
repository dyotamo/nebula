import { check } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import { Socket } from "k6/x/tcp";

const MAGIC = [0x4e, 0x45, 0x42, 0x55];
const VERSION = 1;
const OPCODES = { set: 1, get: 2, delete: 3, ping: 4 };
const MAX_VALUE_SIZE = 8 * 1024 * 1024;
const scenario = __ENV.SCENARIO || "steady";
const workload = {
  steady: { get: 70, set: 30, delete: 0 },
  "read-heavy": { get: 95, set: 5, delete: 0 },
  "write-heavy": { get: 20, set: 80, delete: 0 },
  churn: { get: 34, set: 33, delete: 33 },
  "hot-key": { get: 95, set: 5, delete: 0, hot: true },
  ttl: { get: 70, set: 30, delete: 0, ttl: 2 },
  "large-value": { get: 50, set: 50, delete: 0 },
  ping: { get: 0, set: 0, delete: 0, ping: 100 },
};

if (!workload[scenario]) {
  throw new Error(`Unknown SCENARIO "${scenario}"`);
}

const vus = Number(__ENV.VUS || 16);
const duration = __ENV.DURATION || "30s";
const keyspace = Number(__ENV.KEYSPACE || (scenario === "large-value" ? 128 : 10000));
const valueSize = Number(__ENV.VALUE_SIZE || (scenario === "large-value" ? 1048576 : 128));
const batchSize = Number(__ENV.BATCH_SIZE || 25);
const target = __ENV.TARGET || "nebula-cache-perf";
const port = Number(__ENV.PORT || 17171);
if (
  !Number.isInteger(vus) || vus < 1 ||
  !Number.isInteger(keyspace) || keyspace < 1 ||
  !Number.isInteger(valueSize) || valueSize < 0 || valueSize > MAX_VALUE_SIZE ||
  !Number.isInteger(batchSize) || batchSize < 1
) {
  throw new Error("VUS, KEYSPACE, and BATCH_SIZE must be positive integers; VALUE_SIZE must be 0-8388608");
}
const operationLatency = new Trend("nebula_operation_latency", true);
const operationCount = new Counter("nebula_operations_total");
const operationErrors = new Rate("nebula_operation_errors");
const getMisses = new Counter("nebula_get_misses");
const perOperationMetrics = {};
for (const operation of Object.keys(OPCODES)) {
  perOperationMetrics[operation] = {
    latency: new Trend(`nebula_${operation}_latency`, true),
    count: new Counter(`nebula_${operation}_operations`),
    errors: new Rate(`nebula_${operation}_errors`),
  };
}

export const options = {
  vus,
  duration,
  setupTimeout: "5m",
  thresholds: {
    nebula_operation_errors: ["rate<0.01"],
    checks: ["rate>0.99"],
  },
};

const payload = new Uint8Array(valueSize);
for (let i = 0; i < payload.length; i++) {
  payload[i] = 97 + (i % 26);
}

function u32be(bytes, offset, value) {
  bytes[offset] = (value >>> 24) & 0xff;
  bytes[offset + 1] = (value >>> 16) & 0xff;
  bytes[offset + 2] = (value >>> 8) & 0xff;
  bytes[offset + 3] = value & 0xff;
}

function u64be(bytes, offset, value) {
  for (let i = 7; i >= 0; i--) {
    bytes[offset + i] = value & 0xff;
    value = Math.floor(value / 256);
  }
}

function makeFrame(opcode, key, value, ttl) {
  const frame = new Uint8Array(22 + key.length + value.length);
  frame.set(MAGIC, 0);
  frame[4] = VERSION;
  frame[5] = opcode;
  u32be(frame, 6, key.length);
  u32be(frame, 10, value.length);
  u64be(frame, 14, ttl);
  frame.set(key, 22);
  frame.set(value, 22 + key.length);
  return frame;
}

function makeKey(index) {
  const key = new Uint8Array(16);
  const prefix = [112, 101, 114, 102, 45, 107, 101, 121, 45];
  key.set(prefix, 0);
  let value = index;
  for (let i = 15; i >= 9; i--) {
    key[i] = 48 + (value % 10);
    value = Math.floor(value / 10);
  }
  return key;
}

function connect() {
  const socket = new Socket();
  let buffered = new Uint8Array(0);
  let pending = null;
  let failure = null;
  const closed = new Promise((resolve) => socket.on("close", resolve));
  socket.setTimeout(5000);

  socket.on("data", (data) => {
    const incoming = new Uint8Array(data);
    const combined = new Uint8Array(buffered.length + incoming.length);
    combined.set(buffered);
    combined.set(incoming, buffered.length);
    buffered = combined;
    if (pending && buffered.length >= 10) {
      const length = (
        buffered[6] * 0x1000000 +
        buffered[7] * 0x10000 +
        buffered[8] * 0x100 +
        buffered[9]
      );
      if (length > MAX_VALUE_SIZE) {
        pending.reject(new Error(`Response exceeds ${MAX_VALUE_SIZE} bytes`));
        pending = null;
        socket.destroy();
        return;
      }
      if (buffered.length >= 10 + length) {
        if (
          buffered[0] !== MAGIC[0] || buffered[1] !== MAGIC[1] ||
          buffered[2] !== MAGIC[2] || buffered[3] !== MAGIC[3] ||
          buffered[4] !== VERSION
        ) {
          const error = new Error("Invalid response frame header");
          failure = error;
          pending.reject(error);
          pending = null;
          socket.destroy();
          return;
        }
        const status = buffered[5];
        buffered = buffered.slice(10 + length);
        const resolve = pending.resolve;
        pending = null;
        resolve(status);
      }
    }
  });

  socket.on("error", (error) => {
    failure = error;
    if (pending) {
      pending.reject(error);
      pending = null;
    }
  });
  socket.on("timeout", () => {
    const error = new Error("TCP socket timed out");
    failure = error;
    if (pending) {
      pending.reject(error);
      pending = null;
    }
    socket.destroy();
  });

  const connected = socket.connect(String(port), target);
  return {
    async request(opcode, key, value, ttl) {
      if (failure) {
        throw failure;
      }
      await connected;
      const response = new Promise((resolve, reject) => {
        pending = { resolve, reject };
      });
      try {
        await socket.write(makeFrame(opcode, key, value, ttl).buffer);
      } catch (error) {
        failure = error;
        pending = null;
        socket.destroy();
        throw error;
      }
      return response;
    },
    async close() {
      socket.destroy();
      await closed;
    },
  };
}

export async function setup() {
  if (scenario === "ping") {
    return { ready: true };
  }
  const prefill = connect();
  try {
    for (let index = 0; index < keyspace; index++) {
      const status = await prefill.request(OPCODES.set, makeKey(index), payload, 0);
      if (status !== 0) {
        throw new Error(`Prefill SET failed with status ${status} at key ${index}`);
      }
    }
  } finally {
    await prefill.close();
  }
  return { ready: true };
}

export default async function () {
  const connection = connect();
  try {
    for (let index = 0; index < batchSize; index++) {
      await runOperation(connection);
    }
  } finally {
    await connection.close();
  }
}

async function runOperation(connection) {
  const profile = workload[scenario];
  let operation;
  const random = Math.random() * 100;
  if (random < (profile.ping || 0)) {
    operation = "ping";
  } else if (random < (profile.ping || 0) + profile.get) {
    operation = "get";
  } else if (random < (profile.ping || 0) + profile.get + profile.set) {
    operation = "set";
  } else {
    operation = "delete";
  }

  let keyIndex = Math.floor(Math.random() * keyspace);
  if (profile.hot && Math.random() < 0.9) {
    keyIndex = 0;
  }
  const key = operation === "ping" ? new Uint8Array(0) : makeKey(keyIndex);
  const opcode = OPCODES[operation];
  const ttl = operation === "set" ? (profile.ttl || 0) : 0;
  const started = Date.now();
  let status;
  let succeeded = false;
  try {
    status = await connection.request(opcode, key, operation === "set" ? payload : new Uint8Array(0), ttl);
    const valid = operation === "get" || operation === "delete"
      ? status === 0 || status === 1
      : status === 0;
    succeeded = valid;
    if (operation === "get" && status === 1) {
      getMisses.add(1);
    }
    check(status, {
      [`${operation} returned a valid status`]: () => valid,
    });
  } catch (error) {
    console.error(`${operation} request failed: ${error}`);
    check(null, {
      [`${operation} completed without a protocol error`]: () => false,
    });
  }

  const elapsed = Date.now() - started;
  operationLatency.add(elapsed, { operation });
  operationCount.add(1, { operation });
  operationErrors.add(!succeeded, { operation });
  perOperationMetrics[operation].latency.add(elapsed);
  perOperationMetrics[operation].count.add(1);
  perOperationMetrics[operation].errors.add(!succeeded);
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatValue(value) {
  if (typeof value !== "number") {
    return String(value);
  }
  if (!Number.isFinite(value)) {
    return String(value);
  }
  return Number(value.toPrecision(5)).toLocaleString();
}

function aggregateMetric(metrics, prefix, field) {
  const entries = Object.entries(metrics).filter(([name]) =>
    name === prefix || name.startsWith(`${prefix}{`)
  );
  const exact = entries.find(([name]) => name === prefix);
  if (exact) {
    return exact[1].values[field] || 0;
  }
  return entries.reduce((sum, [, metric]) => sum + (metric.values[field] || 0), 0);
}

function collectChecks(group, rows) {
  for (const item of group.checks || []) {
    rows.push(item);
  }
  for (const child of group.groups || []) {
    collectChecks(child, rows);
  }
}

function renderHtmlReport(summary) {
  const metrics = Object.entries(summary.metrics || {}).sort(([left], [right]) =>
    left.localeCompare(right)
  );
  const metricFields = ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)", "count", "rate", "passes", "fails", "value"];
  const metricRows = metrics.map(([name, metric]) => {
    const values = metricFields
      .filter((field) => metric.values[field] !== undefined)
      .map((field) => `<span><b>${escapeHtml(field)}</b> ${escapeHtml(formatValue(metric.values[field]))}</span>`)
      .join("");
    return `<tr><td>${escapeHtml(name)}</td><td>${escapeHtml(metric.type || "")}</td><td class="stats">${values || "—"}</td></tr>`;
  }).join("");

  const thresholds = [];
  for (const [name, metric] of metrics) {
    for (const [condition, result] of Object.entries(metric.thresholds || {})) {
      const passed = result && typeof result === "object" ? result.ok : Boolean(result);
      thresholds.push(`<tr><td>${escapeHtml(name)}</td><td>${escapeHtml(condition)}</td><td class="${passed ? "pass" : "fail"}">${passed ? "PASS" : "FAIL"}</td></tr>`);
    }
  }
  const checks = [];
  collectChecks(summary.root_group || {}, checks);
  const checkRows = checks.map((item) => {
    const passed = Number(item.fails || 0) === 0;
    return `<tr><td>${escapeHtml(item.name || "")}</td><td>${escapeHtml(item.path || "")}</td><td>${escapeHtml(item.passes || 0)}</td><td>${escapeHtml(item.fails || 0)}</td><td class="${passed ? "pass" : "fail"}">${passed ? "PASS" : "FAIL"}</td></tr>`;
  }).join("");

  const operationCount = aggregateMetric(summary.metrics || {}, "nebula_operations_total", "count");
  const operationRate = aggregateMetric(summary.metrics || {}, "nebula_operations_total", "rate");
  const errorRate = aggregateMetric(summary.metrics || {}, "nebula_operation_errors", "rate");
  const checkCount = checks.reduce((total, item) => total + Number(item.passes || 0) + Number(item.fails || 0), 0);
  const passedChecks = checks.reduce((total, item) => total + Number(item.passes || 0), 0);
  const durationSeconds = Number(summary.state && summary.state.testRunDurationMs || 0) / 1000;
  const generated = new Date().toISOString();
  const reportPath = __ENV.REPORT_PATH || "summary.html";
  const scenarioLabel = escapeHtml(scenario);

  const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NebulaCache k6 report - ${scenarioLabel}</title>
<style>
:root{color-scheme:light;--ink:#17212b;--muted:#607080;--line:#dce4eb;--paper:#f4f7fa;--card:#fff;--blue:#1264a3;--green:#16794b;--red:#b42318}
*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}
main{max-width:1280px;margin:0 auto;padding:32px 22px 56px}h1{margin:0;font-size:30px}h2{font-size:19px;margin:0 0 16px}
.subtitle{color:var(--muted);margin:6px 0 24px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:14px;margin:22px 0 30px}
.card,section{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:18px}.card label{display:block;color:var(--muted);font-size:13px}.card strong{display:block;font-size:24px;margin-top:3px}
section{margin:16px 0;overflow:hidden}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:10px 11px;border-bottom:1px solid var(--line);vertical-align:top}th{background:#f8fafc;color:var(--muted);font-weight:650}
.stats{display:flex;flex-wrap:wrap;gap:6px 13px}.stats span{white-space:nowrap}.pass{color:var(--green);font-weight:700}.fail{color:var(--red);font-weight:700}.empty{color:var(--muted);padding:10px 0}
footer{color:var(--muted);font-size:12px;margin-top:20px}@media(max-width:700px){main{padding:20px 12px}.table-wrap{overflow-x:auto}table{min-width:680px}}
</style>
</head>
<body><main>
<h1>NebulaCache performance report</h1>
<p class="subtitle">Scenario: <b>${scenarioLabel}</b> · Generated ${escapeHtml(generated)} · Target ${escapeHtml(target)}:${escapeHtml(port)}</p>
<div class="cards">
<div class="card"><label>Completed operations</label><strong>${escapeHtml(formatValue(operationCount))}</strong></div>
<div class="card"><label>Operations per second</label><strong>${escapeHtml(formatValue(operationRate))}</strong></div>
<div class="card"><label>Operation error rate</label><strong>${escapeHtml(formatValue(errorRate * 100))}%</strong></div>
<div class="card"><label>Checks passed</label><strong>${escapeHtml(formatValue(passedChecks))} / ${escapeHtml(formatValue(checkCount))}</strong></div>
<div class="card"><label>Measured duration</label><strong>${escapeHtml(formatValue(durationSeconds))} s</strong></div>
<div class="card"><label>Load configuration</label><strong>${escapeHtml(vus)} VUs</strong><span>${escapeHtml(duration)} · ${escapeHtml(keyspace)} keys · ${escapeHtml(valueSize)} B values · batch ${escapeHtml(batchSize)}</span></div>
</div>
<section><h2>Thresholds</h2>${thresholds.length ? `<div class="table-wrap"><table><thead><tr><th>Metric</th><th>Threshold</th><th>Result</th></tr></thead><tbody>${thresholds.join("")}</tbody></table></div>` : '<p class="empty">No threshold results were included.</p>'}</section>
<section><h2>Checks</h2>${checkRows ? `<div class="table-wrap"><table><thead><tr><th>Check</th><th>Group</th><th>Passed</th><th>Failed</th><th>Result</th></tr></thead><tbody>${checkRows}</tbody></table></div>` : '<p class="empty">No checks were recorded.</p>'}</section>
<section><h2>All k6 metrics</h2><div class="table-wrap"><table><thead><tr><th>Metric</th><th>Type</th><th>Statistics</th></tr></thead><tbody>${metricRows}</tbody></table></div></section>
<footer>Generated by the repository's k6 summary hook. Request latency excludes TCP connection setup; operation rate includes amortized connection setup and teardown.</footer>
</main></body></html>`;

  const text = [
    `NebulaCache k6 report: ${scenario}`,
    `Operations: ${formatValue(operationCount)} (${formatValue(operationRate)} ops/s)`,
    `Operation error rate: ${formatValue(errorRate * 100)}%`,
    `Checks: ${formatValue(passedChecks)} / ${formatValue(checkCount)} passed`,
    `HTML report: ${reportPath}`,
  ].join("\n");
  return { [reportPath]: html, stdout: `${text}\n` };
}

export function handleSummary(summary) {
  return renderHtmlReport(summary);
}
