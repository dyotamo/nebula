const assert = require('node:assert/strict');
const net = require('node:net');
const { after, before, test } = require('node:test');
const { setTimeout: delay } = require('node:timers/promises');
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
      while (request.length >= 22) {
        const keyLength = request.readUInt32BE(6);
        const valueLength = request.readUInt32BE(10);
        if (request.length < 22 + keyLength + valueLength) {
          return;
        }

        const opcode = request.readUInt8(5);
        const ttlSecs = request.readBigUInt64BE(14);
        const key = request.subarray(22, 22 + keyLength).toString('utf8');
        const value = request.subarray(22 + keyLength, 22 + keyLength + valueLength);
        request = request.subarray(22 + keyLength + valueLength);
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
        socket.write(Buffer.concat([responseHeader, responseValue]));
      }
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
  await cache.close();
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
  assert.throws(() => new NebulaCache('::1:17171'), TypeError);
  assert.throws(() => new NebulaCache('127.0.0.1:17171', { tls: true }), TypeError);
});

function response(value = Buffer.alloc(0), status = 0) {
  const body = Buffer.from(value);
  const header = Buffer.alloc(10);
  header.write('NEBU', 0, 'ascii');
  header.writeUInt8(1, 4);
  header.writeUInt8(status, 5);
  header.writeUInt32BE(body.length, 6);
  return Buffer.concat([header, body]);
}

async function waitFor(predicate, message, timeout = 2500) {
  const deadline = performance.now() + timeout;
  while (!predicate()) {
    assert.ok(performance.now() < deadline, message);
    await delay(5);
  }
}

async function fixture(t, onRequest, options = {}) {
  const sockets = new Set();
  const accepted = [];
  const requests = [];
  const clients = [];
  const errors = [];
  const tasks = new Set();
  const listener = net.createServer((socket) => {
    sockets.add(socket);
    accepted.push(socket);
    socket.setNoDelay(true);
    socket.on('error', () => {});
    socket.once('close', () => sockets.delete(socket));
    let buffered = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 22) {
        const keyLength = buffered.readUInt32BE(6);
        const valueLength = buffered.readUInt32BE(10);
        const length = 22 + keyLength + valueLength;
        if (buffered.length < length) {
          return;
        }
        const request = {
          opcode: buffered.readUInt8(5),
          key: buffered.subarray(22, 22 + keyLength).toString('utf8'),
          value: Buffer.from(buffered.subarray(22 + keyLength, length)),
          socket,
        };
        buffered = buffered.subarray(length);
        requests.push(request);
        const task = Promise.resolve().then(() => onRequest(request)).catch((error) => {
          errors.push(error);
          socket.destroy();
        });
        tasks.add(task);
        task.finally(() => tasks.delete(task));
      }
    });
  });
  t.after(async () => {
    await Promise.all(clients.map((client) => client.close()));
    for (const socket of sockets) {
      socket.destroy();
    }
    await new Promise((resolve) => listener.close(resolve));
    await Promise.all(tasks);
    assert.deepEqual(errors, []);
  });
  await new Promise((resolve, reject) => {
    listener.once('error', reject);
    listener.listen(0, '127.0.0.1', resolve);
  });
  const address = `127.0.0.1:${listener.address().port}`;
  const createClient = (overrides = {}) => {
    const client = new NebulaCache(address, {
      connectTimeout: 1000,
      requestTimeout: 2000,
      acquireTimeout: 2000,
      ...options,
      ...overrides,
    });
    clients.push(client);
    return client;
  };
  return { client: createClient(), createClient, address, sockets, accepted, requests };
}

test('allows token authentication without TLS', async () => {
  const authenticated = new NebulaCache(`127.0.0.1:${serverAddress.port}`, { token: 'token' });
  await authenticated.close();
});

test('reports connection and server errors', async (t) => {
  const unavailable = net.createServer();
  await new Promise((resolve) => unavailable.listen(0, '127.0.0.1', resolve));
  const address = unavailable.address();
  await new Promise((resolve) => unavailable.close(resolve));
  const disconnected = new NebulaCache(`127.0.0.1:${address.port}`, 100);
  t.after(() => disconnected.close());
  await assert.rejects(disconnected.ping(), CacheConnectionError);
  await assert.rejects(cache.get('failure'), (error) => {
    assert.ok(error instanceof CacheResponseError);
    assert.equal(error.status, 3);
    return true;
  });
});

test('reuses persistent connections sequentially', async (t) => {
  const fixture = await createFixture(t, ({ socket, key }) => {
    socket.write(response(Buffer.from(key || 'PONG')));
  });
  const client = fixture.createClient({ maxPoolSize: 2, maxRetries: 0 });
  assert.deepEqual(await client.get('first'), Buffer.from('first'));
  assert.deepEqual(await client.get('second'), Buffer.from('second'));
  assert.equal(await client.ping(), true);
  assert.equal(fixture.accepted.length, 1);
  assert.equal(fixture.requests.length, 3);
});

test('creates the configured minimum lazily and respects the pool maximum', async (t) => {
  const fixture = await createFixture(t, ({ socket }) => socket.write(response(Buffer.from('PONG'))));
  const client = fixture.createClient({ minPoolSize: 2, maxPoolSize: 2, maxRetries: 0 });
  assert.equal(fixture.accepted.length, 0);
  await client.ping();
  await waitFor(() => fixture.accepted.length === 2, 'minimum pool connections were not initialized');
  for (let index = 0; index < 4; index += 1) {
    await client.ping();
  }
  assert.equal(fixture.accepted.length, 2);
  assert.equal(client._connections.size, 2);
});

test('bounds concurrent requests and acquisition waits', async (t) => {
  let active = 0;
  let peakActive = 0;
  let releaseRequests;
  const released = new Promise((resolve) => { releaseRequests = resolve; });
  let reachedPoolSize;
  const atPoolSize = new Promise((resolve) => { reachedPoolSize = resolve; });
  const fixture = await createFixture(t, async ({ socket, key }) => {
    active += 1;
    peakActive = Math.max(peakActive, active);
    if (peakActive === 2) {
      reachedPoolSize();
    }
    await released;
    socket.write(response(Buffer.from(key)));
    active -= 1;
  });
  const client = fixture.createClient({ maxPoolSize: 2, acquireTimeout: 40, maxRetries: 0 });
  const pending = Array.from({ length: 2 }, (_, index) => client.get(`key-${index}`));
  try {
    await Promise.race([atPoolSize, delay(1000).then(() => { throw new Error('pool did not reach capacity'); })]);
    assert.equal(fixture.accepted.length, 2);
    await assert.rejects(client.ping(), CacheConnectionError);
  } finally {
    releaseRequests();
  }
  const values = await Promise.all(pending);
  for (let index = 2; index < 6; index += 1) {
    values.push(await client.get(`key-${index}`));
  }
  assert.deepEqual(values, Array.from({ length: 6 }, (_, index) => Buffer.from(`key-${index}`)));
  assert.equal(peakActive, 2);
  assert.equal(fixture.accepted.length, 2);
});

test('retries GET transport failures but never replays mutations', async (t) => {
  const fixture = await createFixture(t, ({ socket, opcode, key }) => {
    if (opcode === 2 && fixture.requests.filter((request) => request.key === key).length === 1) {
      socket.destroy();
      return;
    }
    if (opcode === 1) {
      socket.destroy();
      return;
    }
    socket.write(response(Buffer.from(key)));
  });
  const client = fixture.createClient({ maxPoolSize: 1, maxRetries: 2 });
  assert.deepEqual(await client.get('retry'), Buffer.from('retry'));
  assert.equal(fixture.requests.filter((request) => request.key === 'retry').length, 2);
  await assert.rejects(client.set('ambiguous', 'value'), CacheConnectionError);
  assert.equal(fixture.requests.filter((request) => request.opcode === 1).length, 1);
});

test('request timeout retires a connection and the next operation reconnects', async (t) => {
  let first = true;
  const fixture = await createFixture(t, async ({ socket }) => {
    if (first) {
      first = false;
      await delay(100);
    }
    socket.write(response(Buffer.from('PONG')));
  });
  const client = fixture.createClient({ maxPoolSize: 1, requestTimeout: 30, maxRetries: 0 });
  await assert.rejects(client.ping(), CacheConnectionError);
  assert.equal(await client.ping(), true);
  assert.equal(fixture.accepted.length, 2);
});

test('idle expiry replaces pooled connections', async (t) => {
  const fixture = await createFixture(t, ({ socket }) => socket.write(response(Buffer.from('PONG'))));
  const client = fixture.createClient({ maxPoolSize: 1, idleTimeout: 30, maxRetries: 0 });
  await client.ping();
  await delay(60);
  await client.ping();
  assert.equal(fixture.accepted.length, 2);
});

test('close releases pooled sockets and rejects future requests', async (t) => {
  const fixture = await createFixture(t, ({ socket }) => socket.write(response(Buffer.from('PONG'))));
  const client = fixture.createClient({ minPoolSize: 2, maxPoolSize: 2, maxRetries: 0 });
  await client.ping();
  await waitFor(() => fixture.accepted.length === 2, 'minimum pool connections were not initialized');
  await client.close();
  assert.equal(client._connections.size, 0);
  await assert.rejects(client.ping(), CacheConnectionError);
  await assert.rejects(client.get('closed'), CacheConnectionError);
  assert.equal(fixture.requests.length, 1);
});

async function createFixture(t, onRequest, options = {}) {
  const sockets = new Set();
  const accepted = [];
  const requests = [];
  const clients = [];
  const errors = [];
  const tasks = new Set();
  const listener = net.createServer((socket) => {
    sockets.add(socket);
    accepted.push(socket);
    socket.setNoDelay(true);
    socket.on('error', () => {});
    socket.once('close', () => sockets.delete(socket));
    let buffered = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 22) {
        const keyLength = buffered.readUInt32BE(6);
        const valueLength = buffered.readUInt32BE(10);
        const length = 22 + keyLength + valueLength;
        if (buffered.length < length) {
          return;
        }
        const request = {
          opcode: buffered.readUInt8(5),
          key: buffered.subarray(22, 22 + keyLength).toString('utf8'),
          value: Buffer.from(buffered.subarray(22 + keyLength, length)),
          socket,
        };
        buffered = buffered.subarray(length);
        requests.push(request);
        const task = Promise.resolve().then(() => onRequest(request)).catch((error) => {
          errors.push(error);
          socket.destroy();
        });
        tasks.add(task);
        task.finally(() => tasks.delete(task));
      }
    });
  });
  t.after(async () => {
    await Promise.all(clients.map((client) => client.close()));
    for (const socket of sockets) {
      socket.destroy();
    }
    await new Promise((resolve) => listener.close(resolve));
    await Promise.all(tasks);
    assert.deepEqual(errors, []);
  });
  await new Promise((resolve, reject) => {
    listener.once('error', reject);
    listener.listen(0, '127.0.0.1', resolve);
  });
  const address = `127.0.0.1:${listener.address().port}`;
  const createClient = (overrides = {}) => {
    const client = new NebulaCache(address, {
      connectTimeout: 1000,
      requestTimeout: 2000,
      acquireTimeout: 2000,
      ...options,
      ...overrides,
    });
    clients.push(client);
    return client;
  };
  return { createClient, address, sockets, accepted, requests };
}
