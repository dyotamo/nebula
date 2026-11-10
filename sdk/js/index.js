const net = require('node:net');
const tls = require('node:tls');

const MAGIC = Buffer.from('NEBU');
const VERSION = 1;
const REQUEST_HEADER_LENGTH = 22;
const RESPONSE_HEADER_LENGTH = 10;
const MAX_KEY_LENGTH = 1024;
const MAX_VALUE_LENGTH = 8 * 1024 * 1024;
const MAX_UINT64 = 0xffffffffffffffffn;
const STATUS_NAMES = new Map([
  [0, 'OK'],
  [1, 'NOT_FOUND'],
  [2, 'INVALID_ARGUMENT'],
  [3, 'INTERNAL_ERROR'],
  [4, 'UNAUTHORIZED'],
  [5, 'FORBIDDEN'],
]);

/**
 * Connection pool and transport settings. All durations are positive milliseconds.
 * @typedef {Object} NebulaCacheOptions
 * @property {number} [minPoolSize=0] Pool size targeted after the first acquisition.
 * @property {number} [maxPoolSize=10] Maximum connections, including pending connects.
 * @property {number} [connectTimeout=5000] TCP/TLS connection deadline.
 * @property {number} [requestTimeout=5000] Deadline for each request or AUTH exchange.
 * @property {number} [acquireTimeout=5000] Maximum time waiting for a usable connection.
 * @property {number} [idleTimeout=60000] Idle duration before a connection is retired.
 * @property {number} [maxLifetime=300000] Connection age limit; busy connections retire on release.
 * @property {number} [maxRetries=2] Additional GET attempts for transient transport failures.
 * @property {import('node:tls').ConnectionOptions} [tls] Enables TLS when provided.
 * @property {string} [token] Non-empty UTF-8 token of at most 1024 bytes; requires verified TLS.
 */

/** Base class for SDK transport, protocol, and server-response errors. */
class NebulaCacheError extends Error {
  constructor(message) {
    super(message);
    this.name = new.target.name;
  }
}

/** Transport, connection acquisition, or client-closure failure. */
class CacheConnectionError extends NebulaCacheError {}
/** Malformed framing, unknown status, or an unexpected successful PING payload. */
class CacheProtocolError extends NebulaCacheError {}

/** Server error or unexpected authentication result, with numeric `status`. */
class CacheResponseError extends NebulaCacheError {
  /**
   * @param {number} status Numeric protocol status.
   * @param {string} message Decoded server error or authentication failure description.
   */
  constructor(status, message) {
    super(`NebulaCache returned status ${status}: ${message}`);
    this.status = status;
  }
}

/**
 * Asynchronous version 1 binary TCP client with one in-flight request per connection.
 * Public operations reject with SDK errors; invalid inputs use TypeError or RangeError.
 * Constructing a client does not open sockets. Call close() when finished.
 */
class NebulaCache {
  /**
   * @param {string} [address="127.0.0.1:7001"] Host:port, with brackets around IPv6 hosts.
   * @param {number|NebulaCacheOptions} [timeout=5000] Connect/request timeout in
   * milliseconds, or pool options. A numeric timeout leaves acquireTimeout at its default.
   */
  constructor(address = '127.0.0.1:7001', timeout = 5000) {
    const parsedAddress = parseAddress(address);
    this.options = parseOptions(timeout);
    this.host = parsedAddress.host;
    this.port = parsedAddress.port;
    this.address = address;
    this.timeout = this.options.requestTimeout;
    this._connections = new Set();
    this._waiters = [];
    this._retryWaiters = new Set();
    this._closed = false;
    this._retryCapacity = 10;
    this._retryTokens = this._retryCapacity;
    this._retryUpdatedAt = Date.now();
  }

  /**
   * Checks for a successful PONG response; does not retry.
   * @returns {Promise<boolean>} True when the response is valid.
   */
  async ping() {
    const { status, value } = await this._request(4);
    ensureOk(status, value);
    if (!value.equals(Buffer.from('PONG'))) {
      throw new CacheProtocolError('PING response did not contain PONG');
    }

    return true;
  }

  /**
   * Stores a value without retrying writes whose server-side outcome may be unknown.
   * @param {string} key Non-empty key, at most 1024 UTF-8 bytes.
   * @param {string|Uint8Array} value UTF-8 text or raw bytes, at most 8 MiB.
   * @param {number|bigint} [ttlSecs=0] Unsigned 64-bit TTL in seconds; zero disables
   * expiration. Numbers must be safe integers; use bigint for larger TTLs.
   * @returns {Promise<void>} Resolves after the server acknowledges the write.
   */
  async set(key, value, ttlSecs = 0) {
    const encodedKey = encodeKey(key);
    const encodedValue = encodeValue(value);
    if (encodedValue.length > MAX_VALUE_LENGTH) {
      throw new RangeError(`value exceeds the ${MAX_VALUE_LENGTH}-byte protocol limit`);
    }

    const encodedTtl = encodeTtl(ttlSecs);
    const { status, value: responseValue } = await this._request(1, encodedKey, encodedValue, encodedTtl);
    ensureOk(status, responseValue);
  }

  /**
   * Fetches raw bytes, retrying only transient transport failures within the retry budget.
   * @param {string} key Non-empty key, at most 1024 UTF-8 bytes.
   * @returns {Promise<Buffer|null>} The value, or null for NOT_FOUND.
   */
  async get(key) {
    const encodedKey = encodeKey(key);
    const { status, value } = await this._request(2, encodedKey);
    if (status === 1) {
      return null;
    }

    ensureOk(status, value);
    return value;
  }

  /**
   * Removes a key without automatically retrying the deletion.
   * @param {string} key Non-empty key, at most 1024 UTF-8 bytes.
   * @returns {Promise<boolean>} True if removed, false for NOT_FOUND.
   */
  async delete(key) {
    const encodedKey = encodeKey(key);
    const { status, value } = await this._request(3, encodedKey);
    if (status === 1) {
      return false;
    }

    ensureOk(status, value);
    return true;
  }

  /**
   * Permanently closes the client, rejects pending work, and destroys pooled sockets.
   * @returns {Promise<void>} Resolves when sockets close; repeated calls share this promise.
   */
  close() {
    if (this._closePromise) {
      return this._closePromise;
    }

    this._closed = true;
    clearTimeout(this._maintenanceTimer);
    const error = new CacheConnectionError('NebulaCache client is closed');
    for (const waiter of this._waiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    for (const cancel of this._retryWaiters) {
      cancel(error);
    }
    const connections = [...this._connections];
    this._closePromise = Promise.all(connections.map((entry) => entry.closed)).then(() => undefined);
    for (const entry of connections) {
      this._discard(entry, error);
    }
    return this._closePromise;
  }

  _acquire() {
    if (this._closed) {
      return Promise.reject(new CacheConnectionError('NebulaCache client is closed'));
    }
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject };
      waiter.timer = setTimeout(() => {
        this._waiters.splice(this._waiters.indexOf(waiter), 1);
        reject(new CacheConnectionError('Timed out acquiring a NebulaCache connection'));
      }, this.options.acquireTimeout);
      this._waiters.push(waiter);
      this._drain();
    });
  }

  /** @private Assigns idle connections to FIFO waiters and counts connects against pool capacity. */
  _drain() {
    if (this._closed) {
      return;
    }
    const now = Date.now();
    for (const entry of this._connections) {
      if (entry.state !== 'idle') {
        continue;
      }
      if (now - entry.lastUsed >= this.options.idleTimeout
          || now - entry.createdAt >= this.options.maxLifetime) {
        this._discard(entry);
        continue;
      }
      const waiter = this._waiters.shift();
      if (!waiter) {
        break;
      }
      clearTimeout(waiter.timer);
      clearTimeout(entry.idleTimer);
      entry.state = 'busy';
      entry.socket.ref();
      waiter.resolve(entry);
    }
    const connecting = [...this._connections].filter((entry) => entry.state === 'connecting').length;
    const needed = Math.max(this.options.minPoolSize - this._connections.size, this._waiters.length - connecting);
    for (let count = 0; count < needed && this._connections.size < this.options.maxPoolSize; count += 1) {
      this._connect();
    }
  }

  _connect() {
    const socket = this.options.tls
      ? tls.connect({ ...this.options.tls, host: this.host, port: this.port })
      : new net.Socket();
    const entry = { socket, state: 'connecting', createdAt: Date.now() };
    entry.closed = new Promise((resolve) => socket.once('close', resolve));
    this._connections.add(entry);
    entry.connectTimer = setTimeout(() => {
      this._discard(entry, transportError('Connection to NebulaCache timed out'));
    }, this.options.connectTimeout);
    socket.setNoDelay(true);
    socket.on(this.options.tls ? 'secureConnect' : 'connect', () => {
      if (entry.state === 'closed') {
        return;
      }
      clearTimeout(entry.connectTimer);
      entry.lifetimeTimer = setTimeout(() => {
        if (entry.state === 'idle') {
          this._discard(entry);
          this._drain();
        }
      }, this.options.maxLifetime);
      entry.lifetimeTimer.unref();
      if (this.options.token !== undefined) {
        const token = Buffer.from(this.options.token, 'utf8');
        const header = Buffer.alloc(REQUEST_HEADER_LENGTH);
        MAGIC.copy(header, 0);
        header.writeUInt8(VERSION, 4);
        header.writeUInt8(5, 5);
        header.writeUInt32BE(token.length, 10);
        this._exchange(entry, Buffer.concat([header, token]), 5).then(({ status, value }) => {
          if (status !== 0 || !value.equals(Buffer.from('AUTH_RESULT'))) {
            this._discard(entry, new CacheResponseError(status, 'authentication failed'));
          }
        }).catch((error) => this._discard(entry, error));
      } else {
        this._release(entry);
      }
    });
    socket.on('data', (chunk) => {
      if (entry.active) {
        entry.active.data(chunk);
      } else {
        this._discard(entry, new CacheProtocolError('received unsolicited response bytes'));
      }
    });
    socket.on('error', (error) => {
      this._discard(entry, transportError(`Could not reach NebulaCache at ${this.address}: ${error.message}`, error.code));
    });
    socket.on('end', () => this._discard(entry, transportError('connection closed before the response was complete')));
    socket.on('close', () => this._discard(entry, transportError('connection closed before the response was complete')));
    if (!this.options.tls) {
      try {
        socket.connect({ host: this.host, port: this.port });
      } catch (error) {
        this._discard(entry, new CacheConnectionError(error.message));
      }
    }
  }

  /** @private Returns a framed connection to the pool; idle sockets/timers do not keep Node alive. */
  _release(entry) {
    if (entry.state === 'closed') {
      return;
    }
    entry.active = undefined;
    if (this._closed || Date.now() - entry.createdAt >= this.options.maxLifetime) {
      this._discard(entry);
      this._drain();
      return;
    }
    entry.state = 'idle';
    entry.lastUsed = Date.now();
    entry.socket.unref();
    entry.idleTimer = setTimeout(() => {
      this._discard(entry);
      this._drain();
    }, this.options.idleTimeout);
    entry.idleTimer.unref();
    this._drain();
  }

  _discard(entry, error = new CacheConnectionError('NebulaCache connection retired')) {
    if (entry.state === 'closed') {
      return;
    }
    const connecting = entry.state === 'connecting';
    entry.error = error;
    entry.state = 'closed';
    clearTimeout(entry.connectTimer);
    clearTimeout(entry.idleTimer);
    clearTimeout(entry.lifetimeTimer);
    this._connections.delete(entry);
    entry.socket.destroy();
    if (entry.active) {
      entry.active.fail(error);
      entry.active = undefined;
    } else if (connecting && this._waiters.length) {
      const waiter = this._waiters.shift();
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    if (!this._closed) {
      clearTimeout(this._maintenanceTimer);
      this._maintenanceTimer = setTimeout(() => this._drain(), 1000);
      this._maintenanceTimer.unref();
      if (this._waiters.length) {
        this._drain();
      }
    }
  }

  /** @private Consumes the shared ten-token retry budget, refilled at one token per second. */
  _takeRetryToken() {
    const now = Date.now();
    this._retryTokens = Math.min(this._retryCapacity, this._retryTokens + (now - this._retryUpdatedAt) / 1000);
    this._retryUpdatedAt = now;
    if (this._retryTokens < 1) {
      return false;
    }
    this._retryTokens -= 1;
    return true;
  }

  /** @private Full-jitter exponential backoff, capped at one second and canceled by close(). */
  _retryDelay(attempt) {
    return new Promise((resolve, reject) => {
      const cancel = (error) => {
        clearTimeout(timer);
        this._retryWaiters.delete(cancel);
        reject(error);
      };
      const timer = setTimeout(() => {
        this._retryWaiters.delete(cancel);
        resolve();
      }, Math.floor(Math.random() * Math.min(1000, 50 * 2 ** attempt)));
      this._retryWaiters.add(cancel);
    });
  }

  /** @private Encodes big-endian framing once; only GET transport failures may be replayed. */
  async _request(opcode, key = Buffer.alloc(0), value = Buffer.alloc(0), ttlSecs = 0n) {
    const header = Buffer.alloc(REQUEST_HEADER_LENGTH);
    MAGIC.copy(header, 0);
    header.writeUInt8(VERSION, 4);
    header.writeUInt8(opcode, 5);
    header.writeUInt32BE(key.length, 6);
    header.writeUInt32BE(value.length, 10);
    header.writeBigUInt64BE(ttlSecs, 14);
    const request = Buffer.concat([header, key, value]);
    for (let attempt = 0; ; attempt += 1) {
      try {
        const entry = await this._acquire();
        return await this._exchange(entry, request, opcode);
      } catch (error) {
        if (this._closed || opcode !== 2 || !error.transient || attempt >= this.options.maxRetries || !this._takeRetryToken()) {
          throw error;
        }
        await this._retryDelay(attempt);
      }
    }
  }

  /**
   * @private
   * Reassembles fragmented header/body chunks under one request deadline. Invalid or
   * excess bytes discard the socket; complete server-error responses allow reuse.
   */
  _exchange(entry, request, opcode) {
    return new Promise((resolve, reject) => {
      if (this._closed || entry.state === 'closed') {
        reject(entry.error ?? new CacheConnectionError('NebulaCache connection is closed'));
        return;
      }
      let settled = false;
      let headerBytes = 0;
      const responseHeader = Buffer.alloc(RESPONSE_HEADER_LENGTH);
      let expectedValueLength;
      let receivedValueLength = 0;
      const valueChunks = [];
      const timer = setTimeout(() => {
        this._discard(entry, transportError('Request to NebulaCache timed out'));
      }, this.options.requestTimeout);
      const finish = (error, response) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        if (error) {
          reject(error);
        } else {
          this._release(entry);
          resolve(response);
        }
      };
      entry.active = {
        fail: (error) => finish(error),
        data: (chunk) => {
          let offset = 0;
          if (headerBytes < RESPONSE_HEADER_LENGTH) {
            const length = Math.min(RESPONSE_HEADER_LENGTH - headerBytes, chunk.length);
            chunk.copy(responseHeader, headerBytes, 0, length);
            headerBytes += length;
            offset = length;
            if (headerBytes === RESPONSE_HEADER_LENGTH) {
              try {
                expectedValueLength = parseResponseHeader(responseHeader);
              } catch (error) {
                this._discard(entry, error);
                return;
              }
            }
          }
          if (expectedValueLength !== undefined && offset < chunk.length) {
            const valueChunk = chunk.subarray(offset);
            receivedValueLength += valueChunk.length;
            if (receivedValueLength > expectedValueLength) {
              this._discard(entry, new CacheProtocolError('response contains bytes beyond its declared value length'));
              return;
            }
            valueChunks.push(valueChunk);
          }
          if (expectedValueLength !== undefined && receivedValueLength === expectedValueLength) {
            const status = responseHeader.readUInt8(5);
            const value = Buffer.concat(valueChunks, expectedValueLength);
            if (opcode === 4 && status === 0 && !value.equals(Buffer.from('PONG'))) {
              this._discard(entry, new CacheProtocolError('PING response did not contain PONG'));
              return;
            }
            finish(null, { status, value });
          }
        },
      };
      try {
        entry.socket.write(request);
      } catch (error) {
        this._discard(entry, transportError(error.message, error.code));
      }
    });
  }
}

function transportError(message, code) {
  const error = new CacheConnectionError(message);
  error.transient = code === undefined || ['ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EHOSTUNREACH', 'ENETUNREACH', 'EAI_AGAIN'].includes(code);
  return error;
}

function parseOptions(options) {
  if (typeof options === 'number') {
    options = { connectTimeout: options, requestTimeout: options };
  }
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError('options must be a timeout number or an options object');
  }
  const result = {
    minPoolSize: 0,
    maxPoolSize: 10,
    connectTimeout: 5000,
    requestTimeout: 5000,
    acquireTimeout: 5000,
    idleTimeout: 60000,
    maxLifetime: 300000,
    maxRetries: 2,
    tls: undefined,
    token: undefined,
    ...options,
  };
  if (result.tls !== undefined && (!result.tls || typeof result.tls !== 'object' || Array.isArray(result.tls))) {
    throw new TypeError('tls must be a TLS options object when provided');
  }
  if (result.token !== undefined) {
    if (typeof result.token !== 'string' || result.token.length === 0 || Buffer.byteLength(result.token, 'utf8') > 1024) {
      throw new TypeError('token must be a non-empty string no longer than 1024 bytes');
    }
    if (result.tls === undefined || result.tls.rejectUnauthorized === false) {
      throw new TypeError('token authentication requires TLS with certificate verification enabled');
    }
  }
  for (const name of ['minPoolSize', 'maxPoolSize', 'maxRetries']) {
    if (!Number.isSafeInteger(result[name]) || result[name] < (name === 'maxPoolSize' ? 1 : 0)) {
      throw new RangeError(`${name} must be a valid ${name === 'maxPoolSize' ? 'positive' : 'non-negative'} integer`);
    }
  }
  if (result.minPoolSize > result.maxPoolSize) {
    throw new RangeError('minPoolSize must not exceed maxPoolSize');
  }
  for (const name of ['connectTimeout', 'requestTimeout', 'acquireTimeout', 'idleTimeout', 'maxLifetime']) {
    if (typeof result[name] !== 'number' || !Number.isFinite(result[name]) || result[name] <= 0 || result[name] > 2147483647) {
      throw new RangeError(`${name} must be greater than zero and at most 2147483647 milliseconds`);
    }
  }
  return result;
}

function parseAddress(address) {
  if (typeof address !== 'string' || address.length === 0) {
    throw new TypeError('address must be a host:port string');
  }

  let host;
  let portText;
  if (address.startsWith('[')) {
    const closingBracket = address.indexOf(']');
    if (closingBracket < 0 || address[closingBracket + 1] !== ':') {
      throw new TypeError('IPv6 addresses must use the [host]:port format');
    }

    host = address.slice(1, closingBracket);
    portText = address.slice(closingBracket + 2);
  } else {
    if ((address.match(/:/g) || []).length !== 1) {
      throw new TypeError('address must be a host:port string; bracket IPv6 addresses');
    }

    const separator = address.lastIndexOf(':');
    host = address.slice(0, separator);
    portText = address.slice(separator + 1);
  }

  if (!host || !/^\d+$/.test(portText)) {
    throw new TypeError('address must be a host:port string');
  }

  const port = Number(portText);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new RangeError('port must be between 1 and 65535');
  }

  return { host, port };
}

function encodeKey(key) {
  if (typeof key !== 'string') {
    throw new TypeError('key must be a string');
  }

  if (key.length === 0) {
    throw new RangeError('key must not be empty');
  }

  const encodedKey = Buffer.from(key, 'utf8');
  if (encodedKey.length > MAX_KEY_LENGTH) {
    throw new RangeError(`key exceeds the ${MAX_KEY_LENGTH}-byte protocol limit`);
  }

  return encodedKey;
}

function encodeValue(value) {
  if (typeof value === 'string') {
    return Buffer.from(value, 'utf8');
  }

  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value);
  }

  throw new TypeError('value must be a string or Uint8Array');
}

function encodeTtl(ttlSecs) {
  if (typeof ttlSecs === 'number') {
    if (!Number.isSafeInteger(ttlSecs) || ttlSecs < 0) {
      throw new RangeError('ttlSecs must be a non-negative 64-bit integer');
    }
    
    ttlSecs = BigInt(ttlSecs);
  }

  if (typeof ttlSecs !== 'bigint' || ttlSecs < 0n || ttlSecs > MAX_UINT64) {
    throw new RangeError('ttlSecs must be a non-negative 64-bit integer');
  }

  return ttlSecs;
}

/** Validates protocol metadata and the size limit before accepting response body bytes. */
function parseResponseHeader(header) {
  if (!header.subarray(0, 4).equals(MAGIC)) {
    throw new CacheProtocolError('response has invalid magic bytes');
  }

  const version = header.readUInt8(4);
  if (version !== VERSION) {
    throw new CacheProtocolError(`unsupported protocol version: ${version}`);
  }

  const status = header.readUInt8(5);
  if (!STATUS_NAMES.has(status)) {
    throw new CacheProtocolError(`unknown response status: ${status}`);
  }

  const valueLength = header.readUInt32BE(6);
  if (valueLength > MAX_VALUE_LENGTH) {
    throw new CacheProtocolError(`response exceeds the ${MAX_VALUE_LENGTH}-byte protocol limit`);
  }

  return valueLength;
}

function ensureOk(status, value) {
  if (status !== 0) {
    const name = STATUS_NAMES.get(status) ?? `UNKNOWN_${status}`;
    const message = value.toString('utf8').trimEnd();
    throw new CacheResponseError(status, `${name}: ${message}`);
  }
}

module.exports = {
  CacheConnectionError,
  CacheProtocolError,
  CacheResponseError,
  NebulaCache,
  NebulaCacheError,
};
