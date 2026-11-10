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
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
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

/**
 * Thread-safe pooled client for the NebulaCache binary TCP protocol.
 * Connections are opened lazily. Keys use UTF-8 and must occupy 1–1024 bytes;
 * values may contain up to 8 MiB. Only GET requests may be retried after
 * transient I/O failures, subject to configuration and a shared retry budget.
 * Operations report transport, protocol, and server failures through
 * {@link NebulaCacheException}; operations after closure throw
 * {@link IllegalStateException}.
 */
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
    /**
     * Daemon scheduler that closes sockets when an exchange exceeds its request deadline.
     */
    private final ScheduledThreadPoolExecutor deadlines;
    private boolean closed;
    private double retryTokens = 10;
    private long retryRefill = System.nanoTime();

    /**
     * Immutable pool, timeout, retry, TLS, and authentication settings.
     * All durations must be non-null and positive.
     *
     * @param minPoolSize minimum connection count replenished during acquisition, not at construction
     * @param maxPoolSize maximum connection count, including connections being established
     * @param connectTimeout positive TCP connect timeout and TLS handshake read timeout
     * @param requestTimeout positive deadline for each request/response exchange, including authentication
     * @param acquireTimeout positive acquisition budget, checked while waiting and establishing connections
     * @param idleTimeout positive idle age beyond which an idle connection is discarded on acquisition
     * @param maxLifetime positive connection age limit checked on acquisition and release
     * @param maxRetries non-negative maximum additional attempts for transient GET failures
     * @param tlsEnabled whether to use TLS with default trust settings and hostname verification
     * @param token optional UTF-8 authentication token; null disables authentication,
     *     otherwise the token must occupy 1–1024 bytes; TLS is optional
     */
    public record Config(int minPoolSize, int maxPoolSize, Duration connectTimeout,
            Duration requestTimeout, Duration acquireTimeout, Duration idleTimeout,
            Duration maxLifetime, int maxRetries, boolean tlsEnabled, String token) {
        /**
         * Creates an unauthenticated, non-TLS configuration with pool sizes 0–10,
         * five-second connect/request/acquire timeouts, a 60-second idle timeout,
         * a 300-second lifetime, and at most two GET retries.
         */
        public Config() {
            this(0, 10, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
                    Duration.ofSeconds(60), Duration.ofSeconds(300), 2, false, null);
        }

        /**
         * Creates a configuration without token authentication.
         *
         * @param minPoolSize minimum connection count replenished during acquisition, not at construction
         * @param maxPoolSize maximum connection count, including connections being established
         * @param connectTimeout positive TCP connect timeout and TLS handshake read timeout
         * @param requestTimeout positive deadline for each request/response exchange, including authentication
         * @param acquireTimeout positive acquisition budget, checked while waiting and establishing connections
         * @param idleTimeout positive idle age beyond which an idle connection is discarded on acquisition
         * @param maxLifetime positive connection age limit checked on acquisition and release
         * @param maxRetries non-negative maximum additional attempts for transient GET failures
         * @param tlsEnabled whether to enable TLS and hostname verification
         */
        public Config(int minPoolSize, int maxPoolSize, Duration connectTimeout,
                Duration requestTimeout, Duration acquireTimeout, Duration idleTimeout,
                Duration maxLifetime, int maxRetries, boolean tlsEnabled) {
            this(minPoolSize, maxPoolSize, connectTimeout, requestTimeout, acquireTimeout,
                    idleTimeout, maxLifetime, maxRetries, tlsEnabled, null);
        }

        /**
         * Creates a configuration without TLS or token authentication.
         *
         * @param minPoolSize minimum connection count replenished during acquisition, not at construction
         * @param maxPoolSize maximum connection count, including connections being established
         * @param connectTimeout positive TCP connect timeout and TLS handshake read timeout
         * @param requestTimeout positive deadline for each request/response exchange, including authentication
         * @param acquireTimeout positive acquisition budget, checked while waiting and establishing connections
         * @param idleTimeout positive idle age beyond which an idle connection is discarded on acquisition
         * @param maxLifetime positive connection age limit checked on acquisition and release
         * @param maxRetries non-negative maximum additional attempts for transient GET failures
         */
        public Config(int minPoolSize, int maxPoolSize, Duration connectTimeout,
                Duration requestTimeout, Duration acquireTimeout, Duration idleTimeout,
                Duration maxLifetime, int maxRetries) {
            this(minPoolSize, maxPoolSize, connectTimeout, requestTimeout, acquireTimeout,
                    idleTimeout, maxLifetime, maxRetries, false, null);
        }

        /**
         * Validates all settings before constructing the configuration.
         *
         * @param minPoolSize minimum connection count replenished during acquisition, not at construction
         * @param maxPoolSize maximum connection count, including connections being established
         * @param connectTimeout positive TCP connect timeout and TLS handshake read timeout
         * @param requestTimeout positive deadline for each request/response exchange, including authentication
         * @param acquireTimeout positive acquisition budget, checked while waiting and establishing connections
         * @param idleTimeout positive idle age beyond which an idle connection is discarded on acquisition
         * @param maxLifetime positive connection age limit checked on acquisition and release
         * @param maxRetries non-negative maximum additional attempts for transient GET failures
         * @param tlsEnabled whether to enable TLS and hostname verification
         * @param token optional authentication token of 1–1024 UTF-8 bytes
         * @throws IllegalArgumentException if pool sizes, retries, durations, or token settings are invalid
         * @throws NullPointerException if any duration is null
         */
        public Config {
            if (minPoolSize < 0 || maxPoolSize < 1 || minPoolSize > maxPoolSize) {
                throw new IllegalArgumentException("pool sizes must satisfy 0 <= minPoolSize <= maxPoolSize, maxPoolSize >= 1");
            }
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be non-negative");
            }
            if (token != null && (token.isEmpty() || token.getBytes(StandardCharsets.UTF_8).length > 1024)) {
                throw new IllegalArgumentException("token must be non-empty and no longer than 1024 bytes");
            }
            requirePositive(connectTimeout, "connectTimeout");
            requirePositive(requestTimeout, "requestTimeout");
            requirePositive(acquireTimeout, "acquireTimeout");
            requirePositive(idleTimeout, "idleTimeout");
            requirePositive(maxLifetime, "maxLifetime");
        }
    }

    /**
     * Creates a lazy client for {@code 127.0.0.1:17171} with default settings.
     */
    public NebulaCache() {
        this("127.0.0.1:17171");
    }

    /**
     * Creates a lazy client with default settings.
     *
     * @param address endpoint in {@code host:port} or {@code [IPv6]:port} form
     * @throws IllegalArgumentException if the address is malformed or its port is outside 1–65535
     * @throws NullPointerException if address is null
     */
    public NebulaCache(String address) {
        this(address, new Config());
    }

    /**
     * Creates a lazy client with the given connect and request timeouts.
     * Other settings, including the five-second acquisition timeout, use defaults.
     *
     * @param address endpoint in {@code host:port} or {@code [IPv6]:port} form
     * @param timeout positive connect and request timeout
     * @throws IllegalArgumentException if the address or timeout is invalid
     * @throws NullPointerException if address or timeout is null
     */
    public NebulaCache(String address, Duration timeout) {
        this(address, new Config(0, 10, timeout, timeout, DEFAULT_TIMEOUT,
                Duration.ofSeconds(60), Duration.ofSeconds(300), 2));
    }

    /**
     * Creates a lazy client with the supplied settings and a daemon deadline scheduler.
     * No network connection is established until an operation acquires one.
     *
     * @param address endpoint in {@code host:port} or {@code [IPv6]:port} form
     * @param config pool, timeout, retry, TLS, and authentication settings
     * @throws IllegalArgumentException if the address is malformed or its port is outside 1–65535
     * @throws NullPointerException if address or config is null
     */
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

    /**
     * Runs a PING, SET, GET, and DELETE smoke test against a running server.
     * Uses a unique temporary key and prints results to standard output.
     *
     * @param args optional {@code --tls} flag and endpoint; defaults to {@code 127.0.0.1:17171}
     * @throws IllegalArgumentException if arguments are unsupported or the endpoint is invalid
     * @throws NebulaCacheException if a cache operation fails
     * @throws IllegalStateException if the stored smoke-test value cannot be retrieved
     */
    public static void main(String[] args) {
        String address = "127.0.0.1:17171";
        boolean addressSet = false;
        boolean tlsEnabled = false;
        for (String arg : args) {
            if (arg.equals("--tls")) {
                tlsEnabled = true;
            } else if (arg.startsWith("--") || addressSet) {
                throw new IllegalArgumentException("Usage: NebulaCache [--tls] [host:port]");
            } else {
                address = arg;
                addressSet = true;
            }
        }

        Config config = new Config(0, 10, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
                Duration.ofSeconds(60), Duration.ofSeconds(300), 2, tlsEnabled);
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

    /**
     * Checks server responsiveness without retrying the request.
     *
     * @return true after a successful response containing exactly {@code PONG}
     * @throws CacheProtocolException if a successful response does not contain {@code PONG}
     * @throws NebulaCacheException if the request fails
     */
    public boolean ping() {
        Response response = request(4, new byte[0], new byte[0], 0L);
        ensureOk(response);
        if (!java.util.Arrays.equals(response.value(), "PONG".getBytes(StandardCharsets.US_ASCII))) {
            throw new CacheProtocolException("PING response did not contain PONG");
        }
        return true;
    }

    /**
     * Stores a UTF-8 string without expiration.
     * The request is not retried.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @param value non-null string occupying at most 8 MiB in UTF-8
     * @throws IllegalArgumentException if the key or value exceeds protocol limits
     * @throws NullPointerException if key or value is null
     * @throws NebulaCacheException if the request fails
     */
    public void set(String key, String value) {
        set(key, value, BigInteger.ZERO);
    }

    /**
     * Stores a defensive copy of a binary value without expiration.
     * The request is not retried.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @param value non-null value of at most 8 MiB; empty values are allowed
     * @throws IllegalArgumentException if the key or value exceeds protocol limits
     * @throws NullPointerException if key or value is null
     * @throws NebulaCacheException if the request fails
     */
    public void set(String key, byte[] value) {
        set(key, value, BigInteger.ZERO);
    }

    /**
     * Stores a UTF-8 string with an expiration in seconds.
     * The request is not retried.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @param value non-null string occupying at most 8 MiB in UTF-8
     * @param ttlSecs non-negative expiration in seconds; zero disables expiration
     * @throws IllegalArgumentException if the key or value exceeds protocol limits, or TTL is out of range
     * @throws NullPointerException if key or value is null
     * @throws NebulaCacheException if the request fails
     */
    public void set(String key, String value, long ttlSecs) {
        set(key, value, BigInteger.valueOf(ttlSecs));
    }

    /**
     * Stores a defensive copy of a binary value with an expiration in seconds.
     * The request is not retried.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @param value non-null value of at most 8 MiB; empty values are allowed
     * @param ttlSecs non-negative expiration in seconds; zero disables expiration
     * @throws IllegalArgumentException if the key or value exceeds protocol limits, or TTL is out of range
     * @throws NullPointerException if key or value is null
     * @throws NebulaCacheException if the request fails
     */
    public void set(String key, byte[] value, long ttlSecs) {
        set(key, value, BigInteger.valueOf(ttlSecs));
    }

    /**
     * Stores a UTF-8 string with an expiration in seconds.
     * The request is not retried.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @param value non-null string occupying at most 8 MiB in UTF-8
     * @param ttlSecs integer from 0 through 2<sup>64</sup> − 1; zero disables expiration
     * @throws IllegalArgumentException if the key or value exceeds protocol limits, or TTL is out of range
     * @throws NullPointerException if key or value or ttlSecs is null
     * @throws NebulaCacheException if the request fails
     */
    public void set(String key, String value, BigInteger ttlSecs) {
        Objects.requireNonNull(value, "value");
        setBytes(key, value.getBytes(StandardCharsets.UTF_8), ttlSecs);
    }

    /**
     * Stores a defensive copy of a binary value with an expiration in seconds.
     * The request is not retried.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @param value non-null value of at most 8 MiB; empty values are allowed
     * @param ttlSecs integer from 0 through 2<sup>64</sup> − 1; zero disables expiration
     * @throws IllegalArgumentException if the key or value exceeds protocol limits, or TTL is out of range
     * @throws NullPointerException if key or value or ttlSecs is null
     * @throws NebulaCacheException if the request fails
     */
    public void set(String key, byte[] value, BigInteger ttlSecs) {
        Objects.requireNonNull(value, "value");
        setBytes(key, value.clone(), ttlSecs);
    }

    /**
     * Retrieves a binary value, retrying eligible transient I/O failures within configured limits.
     * Protocol errors, server errors, and pool acquisition timeouts are not retried.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @return the value, including an empty byte array, or an empty optional if not found
     * @throws IllegalArgumentException if the key is empty or exceeds the byte limit
     * @throws NullPointerException if key is null
     * @throws NebulaCacheException if the request fails
     */
    public Optional<byte[]> get(String key) {
        Response response = request(2, encodeKey(key), new byte[0], 0L);
        if (response.status() == 1) {
            return Optional.empty();
        }
        ensureOk(response);
        return Optional.of(response.value());
    }

    /**
     * Deletes a key without retrying the request.
     *
     * @param key non-empty key occupying at most 1024 UTF-8 bytes
     * @return true if deleted, or false if the key was not found
     * @throws IllegalArgumentException if the key is empty or exceeds the byte limit
     * @throws NullPointerException if key is null
     * @throws NebulaCacheException if the request fails
     */
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

    /**
     * Leases one connection per attempt and discards it on a transport or protocol failure.
     * Only transient GET I/O failures are retried, with bounded jittered backoff
     * and a client-wide token budget; acquisition timeouts are excluded.
     */
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

    /**
     * Exchanges one protocol frame under a whole-request deadline that also covers writes.
     * The scheduled task closes the socket; synchronizing cancellation on the connection
     * prevents a completed exchange's timeout from closing a subsequently reused socket.
     */
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

    /**
     * Reserves pool slots under the pool lock before opening connections outside it.
     * Replenishes the configured minimum on demand, returning one connection and pooling
     * the rest only after all reservations connect successfully. Failed acquisition
     * releases every reservation; expiration is checked lazily, not by a background reaper.
     */
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
                if (config.tlsEnabled()) {
                    SSLSocket tlsSocket = (SSLSocket) ((SSLSocketFactory) SSLSocketFactory.getDefault())
                            .createSocket(connection.socket, host, port, true);
                    SSLParameters parameters = tlsSocket.getSSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    tlsSocket.setSSLParameters(parameters);
                    tlsSocket.setSoTimeout(millis(Math.min(nanos(config.connectTimeout()), remaining)));
                    tlsSocket.startHandshake();
                    connection.socket = tlsSocket;
                }
                if (config.token() != null) {
                    Response authResponse = exchange(connection, 5,
                            new byte[0], config.token().getBytes(StandardCharsets.UTF_8), 0L);
                    if (authResponse.status() != 0
                            || !java.util.Arrays.equals(authResponse.value(), "AUTH_RESULT".getBytes(StandardCharsets.US_ASCII))) {
                        throw new CacheResponseException(4, "authentication failed");
                    }
                }
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

    /**
     * Consumes from a client-wide bucket holding at most ten retries and refilling one per second.
     */
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

    /**
     * Closes idle and leased sockets, stops the deadline scheduler, and wakes pool waiters.
     * This method is idempotent; subsequent operations fail with {@link IllegalStateException}.
     */
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
        private Socket socket = new Socket();
        private long created;
        private long lastUsed;
    }

    private static final class AcquireTimeoutException extends SocketTimeoutException {
        private AcquireTimeoutException() {
            super("timed out acquiring a pooled connection");
        }
    }

    /**
     * Validates response magic, version, status, and the 8 MiB length limit before allocation.
     * Malformed headers raise protocol errors; truncated responses become socket errors
     * so eligible GET requests can retry them.
     */
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
            if (status < 0 || status > 5) {
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
                case 4 -> "UNAUTHORIZED";
                case 5 -> "FORBIDDEN";
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
