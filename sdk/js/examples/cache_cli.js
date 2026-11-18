const fs = require('node:fs');
const { NebulaCache } = require('../index');

const USAGE = 'Usage: node examples/cache_cli.js [--address host:port] [--tls-ca PEM] <ping|set|get|delete> [arguments]';
const MAX_UINT64 = 0xffffffffffffffffn;

// The CLI accepts unsigned TTLs up to the protocol's 64-bit limit, which exceeds Number's exact integer range.
async function main(argv) {
  // Environment variables supply defaults; leading command-line options below override them.
  let address = process.env.NEBULA_ADDR || '127.0.0.1:17171';
  let tlsCA = process.env.NEBULA_TLS_CA;
  const args = [...argv];

  // Consume address and CA options only before the command, supporting both `--name value` and `--name=value`.

  while (
    args[0] === '--address' || args[0]?.startsWith('--address=') ||
    args[0] === '--tls-ca' || args[0]?.startsWith('--tls-ca=')
  ) {
    const option = args.shift();
    const optionName = option.startsWith('--address') ? '--address' : '--tls-ca';
    // An explicit CLI value replaces its environment default; a missing/empty value is rejected below.
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

  // Help is successful, while invoking the CLI without any command is a usage error.
  if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
    console.log(USAGE);
    return args.length === 0 ? 1 : 0;
  }

  const command = args.shift();
  const options = { minPoolSize: 1, maxPoolSize: 10 };
  // When configured, the CA file contents are passed to the client as TLS trust material.
  if (tlsCA) {
    options.tls = { ca: fs.readFileSync(tlsCA) };
  }
  const cache = new NebulaCache(address, options);

  // Every command path closes the client, including failures during validation or I/O.
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
      // Parse TTL as decimal text directly to BigInt so large valid 64-bit values stay exact.
      // Zero is the default when no TTL option is supplied.
      let ttlSecs = 0n;
      for (let index = 0; index < options.length; index += 1) {
        if (options[index] !== '--ttl' || index + 1 >= options.length) {
          throw new Error(`unexpected set argument: ${options[index]}`);
        }
        // Require unsigned decimal digits and enforce the wire format's uint64 upper bound.
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
      // Display valid UTF-8 as text; fatal decoding avoids silently replacing invalid byte sequences.
      // If decoding fails, hex preserves the original bytes in a printable form.
      try {
        console.log(new TextDecoder('utf-8', { fatal: true }).decode(value));
      } catch {
        console.log(value.toString('hex'));
      }
      return 0;
    }

    if (command === 'delete') {
      requireArgumentCount(args, 1, command);
      // A missing key is a completed delete operation, so this command still exits successfully.
      console.log(await cache.delete(args[0]) ? 'deleted' : 'not found');
      return 0;
    }

    throw new Error(`unknown command: ${command}`);
  } finally {
    await cache.close();
  }
}

// The validators keep command-specific argument errors separate from connection and response errors.
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

// Preserve command-specific exit statuses; operational/usage errors are reported and exit nonzero.
main(process.argv.slice(2)).then((exitCode) => {
  process.exitCode = exitCode;
}).catch((error) => {
  console.error(`nebulacache-example: ${error.message}`);
  console.error(USAGE);
  process.exitCode = 1;
});
