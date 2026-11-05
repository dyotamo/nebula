package io.nebulacache;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class NebulaCache {
    private static final byte[] MAGIC = new byte[] {'N', 'E', 'B', 'U'};
    private static final int VERSION = 1;
    private static final int MAX_KEY_LENGTH = 1024;
    private static final int MAX_VALUE_LENGTH = 8 * 1024 * 1024;
    private static final BigInteger MAX_TTL = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private final String host;
    private final int port;
    private final String address;
    private final int timeoutMillis;

    public NebulaCache() {
        this("127.0.0.1:7001", DEFAULT_TIMEOUT);
    }

    public NebulaCache(String address) {
        this(address, DEFAULT_TIMEOUT);
    }

    public NebulaCache(String address, Duration timeout) {
        this.address = Objects.requireNonNull(address, "address");
        Address parsedAddress = parseAddress(address);
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be greater than zero");
        }

        this.host = parsedAddress.host();
        this.port = parsedAddress.port();
        Duration maxSocketTimeout = Duration.ofMillis(Integer.MAX_VALUE);
        this.timeoutMillis = timeout.compareTo(maxSocketTimeout) >= 0
                ? Integer.MAX_VALUE
                : (int) Math.max(1L, timeout.toMillis());
    }

    public static void main(String[] args) {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: NebulaCache [host:port]");
        }

        NebulaCache cache = args.length == 0 ? new NebulaCache() : new NebulaCache(args[0]);
        String key = "nebula-java-sdk-smoke-" + UUID.randomUUID();
        if (cache.ping()) {
            System.out.println("PING: PONG");
        }

        cache.set(key, "NebulaCache Java SDK smoke test");
        try {
            String value = cache.get(key)
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .orElseThrow(() -> new IllegalStateException("smoke-test value was not found"));
            System.out.println("GET: " + value);
        } finally {
            System.out.println("DELETE: " + cache.delete(key));
        }
    }

    public boolean ping() {
        Response response = request(4, new byte[0], new byte[0], 0L);
        ensureOk(response);
        if (!java.util.Arrays.equals(response.value(), "PONG".getBytes(StandardCharsets.US_ASCII))) {
            throw new CacheProtocolException("PING response did not contain PONG");
        }
        return true;
    }

    public void set(String key, String value) {
        set(key, value, BigInteger.ZERO);
    }

    public void set(String key, byte[] value) {
        set(key, value, BigInteger.ZERO);
    }

    public void set(String key, String value, long ttlSecs) {
        set(key, value, BigInteger.valueOf(ttlSecs));
    }

    public void set(String key, byte[] value, long ttlSecs) {
        set(key, value, BigInteger.valueOf(ttlSecs));
    }

    public void set(String key, String value, BigInteger ttlSecs) {
        Objects.requireNonNull(value, "value");
        setBytes(key, value.getBytes(StandardCharsets.UTF_8), ttlSecs);
    }

    public void set(String key, byte[] value, BigInteger ttlSecs) {
        Objects.requireNonNull(value, "value");
        setBytes(key, value.clone(), ttlSecs);
    }

    public Optional<byte[]> get(String key) {
        Response response = request(2, encodeKey(key), new byte[0], 0L);
        if (response.status() == 1) {
            return Optional.empty();
        }
        ensureOk(response);
        return Optional.of(response.value());
    }

    public boolean delete(String key) {
        Response response = request(3, encodeKey(key), new byte[0], 0L);
        if (response.status() == 1) {
            return false;
        }
        ensureOk(response);
        return true;
    }

    private void setBytes(String key, byte[] value, BigInteger ttlSecs) {
        byte[] encodedKey = encodeKey(key);
        if (value.length > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("value exceeds the " + MAX_VALUE_LENGTH + "-byte protocol limit");
        }
        Objects.requireNonNull(ttlSecs, "ttlSecs");
        if (ttlSecs.signum() < 0 || ttlSecs.compareTo(MAX_TTL) > 0) {
            throw new IllegalArgumentException("ttlSecs must be a non-negative unsigned 64-bit integer");
        }

        Response response = request(1, encodedKey, value, ttlSecs.longValue());
        ensureOk(response);
    }

    private Response request(int opcode, byte[] key, byte[] value, long ttlSecs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);

            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.write(MAGIC);
            output.writeByte(VERSION);
            output.writeByte(opcode);
            output.writeInt(key.length);
            output.writeInt(value.length);
            output.writeLong(ttlSecs);
            output.write(key);
            output.write(value);
            output.flush();

            return readResponse(new DataInputStream(socket.getInputStream()));
        } catch (SocketTimeoutException error) {
            throw new CacheConnectionException("Request to NebulaCache at " + address + " timed out", error);
        } catch (IOException error) {
            throw new CacheConnectionException(
                    "Could not reach NebulaCache at " + address + ": " + error.getMessage(), error);
        }
    }

    private static Response readResponse(DataInputStream input) throws IOException {
        try {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!java.util.Arrays.equals(magic, MAGIC)) {
                throw new CacheProtocolException("response has invalid magic bytes");
            }

            int version = input.readUnsignedByte();
            if (version != VERSION) {
                throw new CacheProtocolException("unsupported protocol version: " + version);
            }

            int status = input.readUnsignedByte();
            if (status < 0 || status > 3) {
                throw new CacheProtocolException("unknown response status: " + status);
            }

            int valueLength = input.readInt();
            if (valueLength < 0 || valueLength > MAX_VALUE_LENGTH) {
                throw new CacheProtocolException(
                        "response exceeds the " + MAX_VALUE_LENGTH + "-byte protocol limit");
            }

            byte[] value = new byte[valueLength];
            input.readFully(value);
            return new Response(status, value);
        } catch (EOFException error) {
            throw new CacheProtocolException("connection closed before the response was complete", error);
        }
    }

    private static byte[] encodeKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
        if (encodedKey.length > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("key exceeds the " + MAX_KEY_LENGTH + "-byte protocol limit");
        }
        return encodedKey;
    }

    private static void ensureOk(Response response) {
        if (response.status() != 0) {
            String statusName = switch (response.status()) {
                case 1 -> "NOT_FOUND";
                case 2 -> "INVALID_ARGUMENT";
                case 3 -> "INTERNAL_ERROR";
                default -> "UNKNOWN_" + response.status();
            };
            String message = new String(response.value(), StandardCharsets.UTF_8).stripTrailing();
            throw new CacheResponseException(response.status(), statusName + ": " + message);
        }
    }

    private static Address parseAddress(String address) {
        if (address.isEmpty()) {
            throw new IllegalArgumentException("address must be a host:port string");
        }

        String host;
        String portText;
        if (address.startsWith("[")) {
            int closingBracket = address.indexOf(']');
            if (closingBracket < 0 || closingBracket + 1 >= address.length()
                    || address.charAt(closingBracket + 1) != ':') {
                throw new IllegalArgumentException("IPv6 addresses must use the [host]:port format");
            }
            host = address.substring(1, closingBracket);
            portText = address.substring(closingBracket + 2);
        } else {
            int separator = address.indexOf(':');
            if (separator < 0 || separator != address.lastIndexOf(':')) {
                throw new IllegalArgumentException("address must be a host:port string; bracket IPv6 addresses");
            }
            host = address.substring(0, separator);
            portText = address.substring(separator + 1);
        }

        if (host.isEmpty() || !portText.matches("[0-9]+")) {
            throw new IllegalArgumentException("address must be a host:port string");
        }

        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException("port must be between 1 and 65535", error);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        return new Address(host, port);
    }

    private record Address(String host, int port) {}

    private record Response(int status, byte[] value) {}
}
