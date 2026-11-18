import { check } from "k6";
import exec from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";
import { Socket } from "k6/x/tcp";

const MAGIC = [0x4e, 0x45, 0x42, 0x55];
const VERSION = 1;
const OPCODES = { set: 1, get: 2, delete: 3, ping: 4, auth: 5 };
const MAX_VALUE_SIZE = 8 * 1024 * 1024;
const MAX_AUTH_TOKEN_SIZE = 1024;
// Scenario weights choose operations; hot and ttl alter key locality and SET expiration separately.
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

// Resolve shared run settings during k6 initialization, before setup or any VU opens a socket.
const vus = Number(__ENV.VUS || 16);
const duration = __ENV.DURATION || "30s";
const keyspace = Number(__ENV.KEYSPACE || (scenario === "large-value" ? 128 : 10000));
const valueSize = Number(__ENV.VALUE_SIZE || (scenario === "large-value" ? 1048576 : 128));
const batchSize = Number(__ENV.BATCH_SIZE || 25);
const target = __ENV.TARGET || "nebula-cache-perf";
const port = Number(__ENV.PORT || 17171);
const authToken = __ENV.AUTH_TOKEN || "";
const authTokenBytes = encodeUtf8(authToken);
if (
  !Number.isInteger(vus) || vus < 1 ||
  !Number.isInteger(keyspace) || keyspace < 1 ||
  !Number.isInteger(valueSize) || valueSize < 0 || valueSize > MAX_VALUE_SIZE ||
  !Number.isInteger(batchSize) || batchSize < 1 ||
  authTokenBytes.length > MAX_AUTH_TOKEN_SIZE
) {
  throw new Error("VUS, KEYSPACE, and BATCH_SIZE must be positive integers; VALUE_SIZE must be 0-8388608; AUTH_TOKEN must be at most 1024 bytes");
}
// Tagged totals drive whole-run thresholds; named operation metrics remain easy to inspect in reports.
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

// Reuse a deterministic payload so per-request timing does not include generating value bytes.
const payload = new Uint8Array(valueSize);
for (let i = 0; i < payload.length; i++) {
  payload[i] = 97 + (i % 26);
}

// Write protocol length fields directly into the frame without platform-dependent byte order.
function u32be(bytes, offset, value) {
  bytes[offset] = (value >>> 24) & 0xff;
  bytes[offset + 1] = (value >>> 16) & 0xff;
  bytes[offset + 2] = (value >>> 8) & 0xff;
  bytes[offset + 3] = value & 0xff;
}

// Token limits count wire bytes, not UTF-16 code units; replace unpaired surrogates as UTF-8 does.
function encodeUtf8(value) {
  const bytes = [];
  for (const character of value) {
    let code = character.codePointAt(0);
    if (code >= 0xd800 && code <= 0xdfff) {
      code = 0xfffd;
    }
    if (code <= 0x7f) {
      bytes.push(code);
    } else if (code <= 0x7ff) {
      bytes.push(0xc0 | (code >> 6), 0x80 | (code & 0x3f));
    } else if (code <= 0xffff) {
      bytes.push(
        0xe0 | (code >> 12),
        0x80 | ((code >> 6) & 0x3f),
        0x80 | (code & 0x3f),
      );
    } else {
      bytes.push(
        0xf0 | (code >> 18),
        0x80 | ((code >> 12) & 0x3f),
        0x80 | ((code >> 6) & 0x3f),
        0x80 | (code & 0x3f),
      );
    }
  }
  return new Uint8Array(bytes);
}

// Encode the numeric TTL into the protocol's eight-byte field, most significant byte first.
function u64be(bytes, offset, value) {
  // Division preserves high bytes that JavaScript's 32-bit shift operators would discard.
  for (let i = 7; i >= 0; i--) {
    bytes[offset + i] = value & 0xff;
    value = Math.floor(value / 256);
  }
}

// Requests have a 22-byte header: NEBU, version, opcode, u32 key/value lengths, u64 TTL.
// All integers are big-endian; key and value bytes follow without terminators.
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

// Fixed-width decimal keys let setup and every VU address the same shared keyspace.
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

// Wrap one socket with response framing, a single in-flight exchange, and lazy authentication.
function connect() {
  const socket = new Socket();
  let buffered = new Uint8Array(0);
  // There are no request IDs on the wire; callers must await each response before sending again.
  let pending = null;
  let failure = null;
  const closed = new Promise((resolve) => socket.on("close", () => {
    if (pending) {
      const message = authToken
        ? "TCP socket closed before receiving a response"
        : "TCP socket closed before receiving a response; set AUTH_TOKEN if the server requires authentication";
      failure = failure || new Error(message);
      pending.reject(failure);
      pending = null;
    }
    resolve();
  }));
  socket.setTimeout(5000);

  // TCP chunks may split frames; wait for the full 10-byte response header and declared body.
  // The response header is NEBU, version, status, then a big-endian u32 body length.
  socket.on("data", (data) => {
    const incoming = new Uint8Array(data);
    const combined = new Uint8Array(buffered.length + incoming.length);
    combined.set(buffered);
    combined.set(incoming, buffered.length);
    buffered = combined;
    if (pending && buffered.length >= 10) {
      // Multiplication keeps the unsigned high length byte from becoming a negative 32-bit integer.
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
        // Workloads measure status and latency rather than value equality; discard the body bytes.
        const status = buffered[5];
        buffered = buffered.slice(10 + length);
        const resolve = pending.resolve;
        pending = null;
        resolve(status);
      }
    }
  });

  // Retain transport failure so later requests in this batch fail instead of reusing a broken stream.
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
  let authenticated = false;
  // Register the response waiter before writing so a fast reply cannot arrive without a consumer.
  async function exchange(opcode, key, value, ttl) {
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
  }

  return {
    // Authenticate on first use only, then reuse the established identity for this batch.
    async request(opcode, key, value, ttl) {
      if (failure) {
        throw failure;
      }
      // Authentication belongs to this socket, not to an individual cache request.
      if (authToken && !authenticated) {
        const status = await exchange(OPCODES.auth, new Uint8Array(0), authTokenBytes, 0);
        if (status !== 0) {
          failure = new Error(`AUTH failed with status ${status}`);
          socket.destroy();
          throw failure;
        }
        authenticated = true;
      }
      return exchange(opcode, key, value, ttl);
    },
    // Await the close event so each iteration finishes releasing its socket before returning.
    async close() {
      socket.destroy();
      await closed;
    },
  };
}

// Prefill outside measured iterations; PING needs no cache contents or setup connection.
export async function setup() {
  if (scenario === "ping") {
    return { ready: true };
  }
  const prefill = connect();
  try {
    // Seed without TTL; the TTL scenario applies expiration to subsequent measured SET operations.
    for (let index = 0; index < keyspace; index++) {
      const status = await prefill.request(OPCODES.set, makeKey(index), payload, 0);
      if (status !== 0) {
        throw new Error(`Prefill SET failed with status ${status} at key ${index}`);
      }
    }
  } catch (error) {
    // Pass setup failure to the VUs so they abort instead of measuring an unpopulated cache.
    return { setupError: String(error) };
  } finally {
    await prefill.close();
  }
  return { ready: true };
}

// Each VU iteration amortizes a fresh connection over a bounded sequence of awaited operations.
export default async function (data) {
  if (data && data.setupError) {
    exec.test.abort(`Setup failed: ${data.setupError}`);
  }
  const connection = connect();
  try {
    for (let index = 0; index < batchSize; index++) {
      await runOperation(connection);
    }
  } finally {
    await connection.close();
  }
}

// Select one workload operation and record both protocol outcomes and end-to-end exchange time.
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
  // The remaining ten percent still samples the full keyspace rather than a separate cold set.
  if (profile.hot && Math.random() < 0.9) {
    keyIndex = 0;
  }
  const key = operation === "ping" ? new Uint8Array(0) : makeKey(keyIndex);
  const opcode = OPCODES[operation];
  const ttl = operation === "set" ? (profile.ttl || 0) : 0;
  // The first exchange may also wait for connection establishment and the lazy AUTH handshake.
  const started = Date.now();
  let status;
  let succeeded = false;
  try {
    status = await connection.request(opcode, key, operation === "set" ? payload : new Uint8Array(0), ttl);
    // NotFound (1) is expected for shared-key deletes and TTL expiry, not a transport failure.
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

  // Failed attempts contribute latency and counts too, preventing errors from disappearing from totals.
  const elapsed = Date.now() - started;
  operationLatency.add(elapsed, { operation });
  operationCount.add(1, { operation });
  operationErrors.add(!succeeded, { operation });
  perOperationMetrics[operation].latency.add(elapsed);
  perOperationMetrics[operation].count.add(1);
  perOperationMetrics[operation].errors.add(!succeeded);
}

// Escape dynamic report values before interpolating them into HTML text or attributes.
function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// Keep finite numeric summaries compact while retaining explicit nonnumeric or nonfinite values.
function formatValue(value) {
  if (typeof value !== "number") {
    return String(value);
  }
  if (!Number.isFinite(value)) {
    return String(value);
  }
  return Number(value.toPrecision(5)).toLocaleString();
}

// Read a summary field from its base metric, falling back to summing matching tagged entries.
function aggregateMetric(metrics, prefix, field) {
  const entries = Object.entries(metrics).filter(([name]) =>
    name === prefix || name.startsWith(`${prefix}{`)
  );
  // Prefer the aggregate over tagged submetrics to avoid counting the same samples twice.
  const exact = entries.find(([name]) => name === prefix);
  if (exact) {
    return exact[1].values[field] || 0;
  }
  return entries.reduce((sum, [, metric]) => sum + (metric.values[field] || 0), 0);
}

// k6 nests checks under groups; flatten the tree so the report includes checks at every depth.
function collectChecks(group, rows) {
  for (const item of group.checks || []) {
    rows.push(item);
  }
  for (const child of group.groups || []) {
    collectChecks(child, rows);
  }
}

// Build the k6 output mapping in memory; k6 writes the selected report path and stdout output.
function renderHtmlReport(summary) {
  const metrics = Object.entries(summary.metrics || {}).sort(([left], [right]) =>
    left.localeCompare(right)
  );
  // Metric kinds expose different fields, so omit absent statistics rather than inventing zeroes.
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
      // Accept both structured threshold results and the boolean form used by some summaries.
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

// Override k6's summary hook with matching HTML and concise terminal views of the completed run.
export function handleSummary(summary) {
  return renderHtmlReport(summary);
}
