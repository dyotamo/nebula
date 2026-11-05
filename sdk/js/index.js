const net = require('node:net');

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
]);

class NebulaCacheError extends Error {
  constructor(message) {
    super(message);
    this.name = new.target.name;
  }
}

class CacheConnectionError extends NebulaCacheError {}
class CacheProtocolError extends NebulaCacheError {}

class CacheResponseError extends NebulaCacheError {
  constructor(status, message) {
    super(`NebulaCache returned status ${status}: ${message}`);
    this.status = status;
  }
}

class NebulaCache {
  constructor(address = '127.0.0.1:7001', timeout = 5000) {
    const parsedAddress = parseAddress(address);
    if (typeof timeout !== 'number' || !Number.isFinite(timeout) || timeout <= 0) {
      throw new RangeError('timeout must be greater than zero');
    }

    this.host = parsedAddress.host;
    this.port = parsedAddress.port;
    this.address = address;
    this.timeout = timeout;
  }

  async ping() {
    const { status, value } = await this._request(4);
    ensureOk(status, value);
    if (!value.equals(Buffer.from('PONG'))) {
      throw new CacheProtocolError('PING response did not contain PONG');
    }

    return true;
  }

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

  async get(key) {
    const encodedKey = encodeKey(key);
    const { status, value } = await this._request(2, encodedKey);
    if (status === 1) {
      return null;
    }

    ensureOk(status, value);
    return value;
  }

  async delete(key) {
    const encodedKey = encodeKey(key);
    const { status, value } = await this._request(3, encodedKey);
    if (status === 1) {
      return false;
    }

    ensureOk(status, value);
    return true;
  }

  _request(opcode, key = Buffer.alloc(0), value = Buffer.alloc(0), ttlSecs = 0n) {
    const header = Buffer.alloc(REQUEST_HEADER_LENGTH);
    MAGIC.copy(header, 0);
    header.writeUInt8(VERSION, 4);
    header.writeUInt8(opcode, 5);
    header.writeUInt32BE(key.length, 6);
    header.writeUInt32BE(value.length, 10);
    header.writeBigUInt64BE(ttlSecs, 14);
    const request = Buffer.concat([header, key, value]);
    const socket = net.createConnection({ host: this.host, port: this.port });

    return new Promise((resolve, reject) => {
      let settled = false;
      let headerBytes = 0;
      let responseHeader;
      let expectedValueLength;
      let receivedValueLength = 0;
      const valueChunks = [];

      const finish = (error, response) => {
        if (settled) {
          return;
        }

        settled = true;
        socket.setTimeout(0);
        socket.destroy();

        if (error) {
          reject(error);
        } else {
          resolve(response);
        }
      };

      socket.setTimeout(this.timeout, () => {
        finish(new CacheConnectionError(`Request to NebulaCache at ${this.address} timed out`));
      });

      socket.on('connect', () => socket.write(request));
      socket.on('data', (chunk) => {
        let offset = 0;
        if (headerBytes < RESPONSE_HEADER_LENGTH) {
          const headerChunkLength = Math.min(RESPONSE_HEADER_LENGTH - headerBytes, chunk.length);
          chunk.copy(responseHeader ??= Buffer.alloc(RESPONSE_HEADER_LENGTH), headerBytes, 0, headerChunkLength);
          headerBytes += headerChunkLength;
          offset = headerChunkLength;

          if (headerBytes === RESPONSE_HEADER_LENGTH) {
            try {
              expectedValueLength = parseResponseHeader(responseHeader);
            } catch (error) {
              finish(error);
              return;
            }
          }
        }

        if (expectedValueLength !== undefined && offset < chunk.length) {
          const valueChunk = chunk.subarray(offset);
          receivedValueLength += valueChunk.length;

          if (receivedValueLength > expectedValueLength) {
            finish(new CacheProtocolError('response contains bytes beyond its declared value length'));
            return;
          }

          valueChunks.push(valueChunk);
        }

        if (expectedValueLength !== undefined && receivedValueLength === expectedValueLength) {
          const status = responseHeader.readUInt8(5);
          finish(null, { status, value: Buffer.concat(valueChunks, expectedValueLength) });
        }
      });

      socket.on('end', () => {
        if (!settled) {
          finish(new CacheProtocolError('connection closed before the response was complete'));
        }
      });

      socket.on('error', (error) => {
        finish(new CacheConnectionError(`Could not reach NebulaCache at ${this.address}: ${error.message}`));
      });

      socket.on('close', () => {
        if (!settled) {
          finish(new CacheProtocolError('connection closed before the response was complete'));
        }
      });
    });
  }
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
