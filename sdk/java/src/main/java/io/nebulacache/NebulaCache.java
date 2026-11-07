package io.nebulacache;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class NebulaCache implements AutoCloseable {
    private static final byte[] MAGIC = new byte[] {'N', 'E', 'B', 'U'};
    private static final int VERSION = 1;
    private static final int MAX_KEY_LENGTH = 1024;
    private static final int MAX_VALUE_LENGTH = 8 * 1024 * 1024;
    private static final BigInteger MAX_TTL = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private final String host;
    private final int port;
    private final String address;
    private final Config config;
    private final Object poolLock = new Object();
    private final ArrayDeque<Connection> idle = new ArrayDeque<>();
    private final Set<Connection> connections = new HashSet<>();
    private final ScheduledThreadPoolExecutor deadlines;
    private boolean closed;
    private double retryTokens = 10;
    private long retryRefill = System.nanoTime();

    public record Config(int minPoolSize, int maxPoolSize, Duration connectTimeout,
            Duration requestTimeout, Duration acquireTimeout, Duration idleTimeout,
            Duration maxLifetime, int maxRetries) {
        public Config() {
            this(0, 10, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
                    Duration.ofSeconds(60), Duration.ofSeconds(300), 2);
        }

        public Config {
            if (minPoolSize < 0 || maxPoolSize < 1 || minPoolSize > maxPoolSize) {
                throw new IllegalArgumentException("pool sizes must satisfy 0 <= minPoolSize <= maxPoolSize, maxPoolSize >= 1");
            }
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be non-negative");
            }
            requirePositive(connectTimeout, "connectTimeout");
            requirePositive(requestTimeout, "requestTimeout");
            requirePositive(acquireTimeout, "acquireTimeout");
            requirePositive(idleTimeout, "idleTimeout");
            requirePositive(maxLifetime, "maxLifetime");
        }
    }

    public NebulaCache() {
        this("127.0.0.1:7001");
    }

    public NebulaCache(String address) {
        this(address, new Config());
    }

    public NebulaCache(String address, Duration timeout) {
        this(address, new Config(0, 10, timeout, timeout, DEFAULT_TIMEOUT,
                Duration.ofSeconds(60), Duration.ofSeconds(300), 2));
    }

    public NebulaCache(String address, Config config) {
        this.address = Objects.requireNonNull(address, "address");
        Address parsedAddress = parseAddress(address);
        this.config = Objects.requireNonNull(config, "config");
        this.host = parsedAddress.host();
        this.port = parsedAddress.port();
        deadlines = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "nebulacache-deadlines");
            thread.setDaemon(true);
            return thread;
        });
        deadlines.setRemoveOnCancelPolicy(true);
    }

    private static void requirePositive(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException error) {
            return Long.MAX_VALUE;
        }
    }

    private static int millis(long nanos) {
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1, TimeUnit.NANOSECONDS.toMillis(nanos)));
    }

    public static void main(String[] args) {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: NebulaCache [host:port]");
        }

        String address = args.length == 0 ? "127.0.0.1:7001" : args[0];
        Config config = new Config(0, 10, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
                Duration.ofSeconds(60), Duration.ofSeconds(300), 2);
        try (NebulaCache cache = new NebulaCache(address, config)) {
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
        for (int attempt = 0; ; attempt++) {
            Connection connection = null;
            boolean reusable = false;
            try {
                connection = acquire();
                Response response = exchange(connection, opcode, key, value, ttlSecs);
                reusable = true;
                return response;
            } catch (IOException error) {
                if (connection != null) {
                    release(connection, false);
                    connection = null;
                }
                synchronized (poolLock) {
                    ensureOpen();
                }
                if (opcode != 2 || attempt >= config.maxRetries() || !isTransient(error) || !takeRetryToken()) {
                    throw new CacheConnectionException(
                            "Request to NebulaCache at " + address + " failed: " + error.getMessage(), error);
                }
                backoff(attempt);
            } finally {
                if (connection != null) {
                    release(connection, reusable);
                }
            }
        }
    }

    private Response exchange(Connection connection, int opcode, byte[] key, byte[] value, long ttlSecs)
            throws IOException {
        ScheduledFuture<?> timeout;
        boolean[] active = {true};
        long started = System.nanoTime();
        synchronized (poolLock) {
            ensureOpen();
            timeout = deadlines.schedule(() -> {
                synchronized (connection) {
                    if (active[0]) {
                        closeSocket(connection.socket);
                    }
                }
            }, nanos(config.requestTimeout()), TimeUnit.NANOSECONDS);
        }
        try {
            connection.socket.setSoTimeout(millis(nanos(config.requestTimeout())));
            DataOutputStream output = new DataOutputStream(connection.socket.getOutputStream());
            output.write(MAGIC);
            output.writeByte(VERSION);
            output.writeByte(opcode);
            output.writeInt(key.length);
            output.writeInt(value.length);
            output.writeLong(ttlSecs);
            output.write(key);
            output.write(value);
            output.flush();
            Response response = readResponse(new DataInputStream(connection.socket.getInputStream()));
            if (System.nanoTime() - started >= nanos(config.requestTimeout())) {
                throw new SocketTimeoutException("request timed out");
            }
            return response;
        } finally {
            synchronized (connection) {
                active[0] = false;
                timeout.cancel(false);
            }
        }
    }

    private Connection acquire() throws IOException {
        long started = System.nanoTime();
        List<Connection> reserved = new ArrayList<>();
        synchronized (poolLock) {
            while (true) {
                ensureOpen();
                long now = System.nanoTime();
                idle.removeIf(connection -> {
                    if (expired(connection, now)) {
                        connections.remove(connection);
                        closeSocket(connection.socket);
                        return true;
                    }
                    return false;
                });
                int needed = Math.max(0, config.minPoolSize() - connections.size());
                if (needed == 0 && !idle.isEmpty()) {
                    return idle.removeFirst();
                }
                if (needed > 0 || connections.size() < config.maxPoolSize()) {
                    for (int index = 0; index < Math.max(1, needed); index++) {
                        Connection connection = new Connection();
                        connections.add(connection);
                        reserved.add(connection);
                    }
                    break;
                }
                long remaining = nanos(config.acquireTimeout()) - (now - started);
                if (remaining <= 0) {
                    throw new AcquireTimeoutException();
                }
                waitForPool(remaining);
            }
        }
        boolean connected = false;
        try {
            for (Connection connection : reserved) {
                long remaining = nanos(config.acquireTimeout()) - (System.nanoTime() - started);
                if (remaining <= 0) {
                    throw new AcquireTimeoutException();
                }
                connection.socket.connect(new InetSocketAddress(host, port),
                        millis(Math.min(nanos(config.connectTimeout()), remaining)));
                connection.socket.setTcpNoDelay(true);
                connection.created = System.nanoTime();
                connection.lastUsed = connection.created;
            }
            synchronized (poolLock) {
                ensureOpen();
                if (System.nanoTime() - started >= nanos(config.acquireTimeout())) {
                    throw new AcquireTimeoutException();
                }
                for (int index = 1; index < reserved.size(); index++) {
                    idle.addLast(reserved.get(index));
                }
                connected = true;
                poolLock.notifyAll();
                return reserved.get(0);
            }
        } finally {
            if (!connected) {
                for (Connection connection : reserved) {
                    release(connection, false);
                }
            }
        }
    }

    private boolean expired(Connection connection, long now) {
        return connection.socket.isClosed()
                || now - connection.created >= nanos(config.maxLifetime())
                || now - connection.lastUsed >= nanos(config.idleTimeout());
    }

    private void release(Connection connection, boolean reusable) {
        synchronized (poolLock) {
            long now = System.nanoTime();
            connection.lastUsed = now;
            if (reusable && !closed && !expired(connection, now)) {
                idle.addLast(connection);
            } else {
                connections.remove(connection);
                closeSocket(connection.socket);
            }
            poolLock.notifyAll();
        }
    }

    private static boolean isTransient(IOException error) {
        return !(error instanceof AcquireTimeoutException)
                && (error instanceof SocketException || error instanceof SocketTimeoutException
                        || error instanceof EOFException);
    }

    private boolean takeRetryToken() {
        synchronized (poolLock) {
            long now = System.nanoTime();
            retryTokens = Math.min(10, retryTokens + (now - retryRefill) / 1_000_000_000.0);
            retryRefill = now;
            if (retryTokens < 1) {
                return false;
            }
            retryTokens--;
            return true;
        }
    }

    private void backoff(int attempt) {
        long ceilingMillis = Math.min(1000, 50L << Math.min(attempt, 5));
        long duration = TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(ceilingMillis + 1));
        long deadline = System.nanoTime() + duration;
        synchronized (poolLock) {
            long remaining = duration;
            while (remaining > 0) {
                ensureOpen();
                waitForPool(remaining);
                remaining = deadline - System.nanoTime();
            }
            ensureOpen();
        }
    }

    private void waitForPool(long remaining) {
        try {
            TimeUnit.NANOSECONDS.timedWait(poolLock, remaining);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new CacheConnectionException("Interrupted while waiting for NebulaCache", error);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("NebulaCache is closed");
        }
    }

    @Override
    public void close() {
        synchronized (poolLock) {
            if (closed) {
                return;
            }
            closed = true;
            for (Connection connection : connections) {
                closeSocket(connection.socket);
            }
            connections.clear();
            idle.clear();
            deadlines.shutdownNow();
            poolLock.notifyAll();
        }
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private static final class Connection {
        private final Socket socket = new Socket();
        private long created;
        private long lastUsed;
    }

    private static final class AcquireTimeoutException extends SocketTimeoutException {
        private AcquireTimeoutException() {
            super("timed out acquiring a pooled connection");
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
            SocketException connectionError = new SocketException("connection closed before the response was complete");
            connectionError.initCause(error);
            throw connectionError;
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
