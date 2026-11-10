const fs = require('node:fs');
const { NebulaCache } = require('../index');

const USAGE = 'Usage: node examples/cache_cli.js [--address host:port] [--tls-ca PEM] <ping|set|get|delete> [arguments]';
const MAX_UINT64 = 0xffffffffffffffffn;

async function main(argv) {
  let address = process.env.NEBULA_ADDR || '127.0.0.1:17171';
  let tlsCA = process.env.NEBULA_TLS_CA;
  const args = [...argv];

  while (
    args[0] === '--address' || args[0]?.startsWith('--address=') ||
    args[0] === '--tls-ca' || args[0]?.startsWith('--tls-ca=')
  ) {
    const option = args.shift();
    const optionName = option.startsWith('--address') ? '--address' : '--tls-ca';
    const value = option === optionName ? args.shift() : option.slice(`${optionName}=`.length);
    if (!value) {
      throw new Error(`${optionName} requires a value`);
    }
    if (optionName === '--address') {
      address = value;
    } else {
      tlsCA = value;
    }
  }

  if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
    console.log(USAGE);
    return args.length === 0 ? 1 : 0;
  }

  const command = args.shift();
  const options = { minPoolSize: 1, maxPoolSize: 10 };
  if (tlsCA) {
    options.tls = { ca: fs.readFileSync(tlsCA) };
  }
  const cache = new NebulaCache(address, options);

  try {
    if (command === 'ping') {
      requireNoArguments(args);
      await cache.ping();
      console.log('PONG');
      return 0;
    }

    if (command === 'set') {
      const [key, value, ...options] = args;
      if (key === undefined || value === undefined) {
        throw new Error('set requires a key and value');
      }
      let ttlSecs = 0n;
      for (let index = 0; index < options.length; index += 1) {
        if (options[index] !== '--ttl' || index + 1 >= options.length) {
          throw new Error(`unexpected set argument: ${options[index]}`);
        }
        const ttlText = options[index + 1];
        if (!/^\d+$/.test(ttlText)) {
          throw new Error('TTL must be a non-negative 64-bit integer');
        }
        ttlSecs = BigInt(ttlText);
        if (ttlSecs > MAX_UINT64) {
          throw new Error('TTL must be a non-negative 64-bit integer');
        }
        index += 1;
      }
      await cache.set(key, value, ttlSecs);
      console.log('stored');
      return 0;
    }

    if (command === 'get') {
      requireArgumentCount(args, 1, command);
      const value = await cache.get(args[0]);
      if (value === null) {
        console.log('not found');
        return 1;
      }
      try {
        console.log(new TextDecoder('utf-8', { fatal: true }).decode(value));
      } catch {
        console.log(value.toString('hex'));
      }
      return 0;
    }

    if (command === 'delete') {
      requireArgumentCount(args, 1, command);
      console.log(await cache.delete(args[0]) ? 'deleted' : 'not found');
      return 0;
    }

    throw new Error(`unknown command: ${command}`);
  } finally {
    await cache.close();
  }
}

function requireNoArguments(args) {
  if (args.length !== 0) {
    throw new Error(`unexpected argument: ${args[0]}`);
  }
}

function requireArgumentCount(args, count, command) {
  if (args.length !== count) {
    throw new Error(`${command} requires ${count} argument${count === 1 ? '' : 's'}`);
  }
}

main(process.argv.slice(2)).then((exitCode) => {
  process.exitCode = exitCode;
}).catch((error) => {
  console.error(`nebulacache-example: ${error.message}`);
  console.error(USAGE);
  process.exitCode = 1;
});
