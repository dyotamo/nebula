const assert = require('node:assert/strict');
const net = require('node:net');
const { after, before, test } = require('node:test');
const {
  CacheConnectionError,
  CacheProtocolError,
  CacheResponseError,
  NebulaCache,
} = require('../index');

const entries = new Map();
let server;
let cache;
let serverAddress;

before(async () => {
  server = net.createServer((socket) => {
    let request = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      request = Buffer.concat([request, chunk]);
      if (request.length < 22) {
        return;
      }
      const keyLength = request.readUInt32BE(6);
      const valueLength = request.readUInt32BE(10);
      if (request.length < 22 + keyLength + valueLength) {
        return;
      }

      const opcode = request.readUInt8(5);
      const ttlSecs = request.readBigUInt64BE(14);
      const key = request.subarray(22, 22 + keyLength).toString('utf8');
      const value = request.subarray(22 + keyLength, 22 + keyLength + valueLength);
      let status = 0;
      let responseValue = Buffer.alloc(0);

      if (opcode === 4) {
        responseValue = Buffer.from('PONG');
      } else if (opcode === 1) {
        entries.set(key, { value: Buffer.from(value), ttlSecs });
      } else if (opcode === 2 && key === 'failure') {
        status = 3;
        responseValue = Buffer.from('forced failure');
      } else if (opcode === 2 && entries.has(key)) {
        responseValue = entries.get(key).value;
      } else if (opcode === 2) {
        status = 1;
      } else if (opcode === 3 && entries.has(key)) {
        entries.delete(key);
      } else if (opcode === 3) {
        status = 1;
      } else {
        status = 2;
        responseValue = Buffer.from('invalid request');
      }

      const responseHeader = Buffer.alloc(10);
      responseHeader.write('NEBU', 0, 'ascii');
      responseHeader.writeUInt8(1, 4);
      responseHeader.writeUInt8(status, 5);
      responseHeader.writeUInt32BE(responseValue.length, 6);
      socket.end(Buffer.concat([responseHeader, responseValue]));
    });
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  serverAddress = server.address();
  cache = new NebulaCache(`127.0.0.1:${serverAddress.port}`);
});

after(async () => {
  await new Promise((resolve) => server.close(resolve));
});

test('ping and key lifecycle', async () => {
  entries.clear();
  assert.equal(await cache.ping(), true);
  await cache.set('greeting', 'hello', 30);
  assert.deepEqual(await cache.get('greeting'), Buffer.from('hello'));
  assert.equal(entries.get('greeting').ttlSecs, 30n);
  assert.equal(await cache.delete('greeting'), true);
  assert.equal(await cache.delete('greeting'), false);
  assert.equal(await cache.get('greeting'), null);
});

test('preserves binary values, UTF-8 keys, and empty values', async () => {
  entries.clear();
  const key = 'space / café';
  const value = Buffer.from([0, 255]);
  await cache.set(key, value);
  assert.deepEqual(await cache.get(key), value);
  await cache.set('empty', new Uint8Array());
  assert.deepEqual(await cache.get('empty'), Buffer.alloc(0));
});

test('rejects invalid arguments', async () => {
  await assert.rejects(cache.set('', 'value'), RangeError);
  await assert.rejects(cache.set('key', 'value', -1), RangeError);
  await assert.rejects(cache.set('key', 'value', 1.5), RangeError);
  await assert.rejects(cache.set('key', 1), TypeError);
  await assert.rejects(cache.set('é'.repeat(513), 'value'), RangeError);
  assert.throws(() => new NebulaCache('localhost:70000'), RangeError);
  assert.throws(() => new NebulaCache('::1:7001'), TypeError);
});

test('reports connection and server errors', async () => {
  await assert.rejects(new NebulaCache(`127.0.0.1:${serverAddress.port + 1}`, 100).ping(), CacheConnectionError);
  await assert.rejects(cache.get('failure'), (error) => {
    assert.ok(error instanceof CacheResponseError);
    assert.equal(error.status, 3);
    return true;
  });
});

test('rejects malformed protocol responses', async () => {
  const malformedServer = net.createServer((socket) => {
    let requestLength = 0;
    let responded = false;
    socket.on('data', (chunk) => {
      requestLength += chunk.length;
      if (requestLength < 22 || responded) {
        return;
      }
      responded = true;
      const response = Buffer.alloc(10);
      response.write('NOPE', 0, 'ascii');
      response.writeUInt8(1, 4);
      response.writeUInt8(0, 5);
      response.writeUInt32BE(0, 6);
      socket.end(response);
    });
  });
  await new Promise((resolve) => malformedServer.listen(0, '127.0.0.1', resolve));
  const address = malformedServer.address();
  try {
    await assert.rejects(new NebulaCache(`127.0.0.1:${address.port}`).ping(), CacheProtocolError);
  } finally {
    await new Promise((resolve) => malformedServer.close(resolve));
  }
});
