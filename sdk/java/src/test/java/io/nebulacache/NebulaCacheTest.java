package io.nebulacache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class NebulaCacheTest {
    private static final byte[] MAGIC = new byte[] {'N', 'E', 'B', 'U'};
    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();
    private static TestServer server;
    private static NebulaCache cache;

    @BeforeAll
    static void startServer() throws IOException {
        server = new TestServer(NebulaCacheTest::respond);
        cache = new NebulaCache(server.address());
    }

    @AfterAll
    static void stopServer() throws Exception {
        cache.close();
        server.close();
    }

    @BeforeEach
    void clearEntries() {
        ENTRIES.clear();
    }

    @Test
    void supportsPingAndKeyLifecycle() {
        assertTrue(cache.ping());
        cache.set("greeting", "hello", 30);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), cache.get("greeting").orElseThrow());
        assertEquals(BigInteger.valueOf(30), ENTRIES.get("greeting").ttlSecs());
        assertTrue(cache.delete("greeting"));
        assertFalse(cache.delete("greeting"));
        assertTrue(cache.get("greeting").isEmpty());
    }

    @Test
    void preservesBinaryValuesUtf8KeysAndEmptyValues() {
        String key = "space / café";
        byte[] value = new byte[] {0, (byte) 0xff};
        cache.set(key, value);
        assertArrayEquals(value, cache.get(key).orElseThrow());
        cache.set("empty", new byte[0]);
        assertArrayEquals(new byte[0], cache.get("empty").orElseThrow());
    }

    @Test
    void encodesFullUnsignedTtlRange() {
        BigInteger ttl = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        cache.set("max-ttl", "value", ttl);
        assertEquals(ttl, ENTRIES.get("max-ttl").ttlSecs());
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> cache.set("", "value"));
        assertThrows(IllegalArgumentException.class, () -> cache.set("key", "value", -1L));
        assertThrows(IllegalArgumentException.class,
                () -> cache.set("key", "value", BigInteger.ONE.shiftLeft(64)));
        assertThrows(IllegalArgumentException.class, () -> cache.set("é".repeat(513), "value"));
        assertThrows(IllegalArgumentException.class, () -> new NebulaCache("localhost:70000"));
        assertThrows(IllegalArgumentException.class, () -> new NebulaCache("::1:17171"));
        assertThrows(IllegalArgumentException.class, () -> new NebulaCache("127.0.0.1:17171", Duration.ZERO));
    }

    @Test
    void mainRunsSdkSmokeFlow() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOutput = System.out;
        try (PrintStream capturedOutput = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capturedOutput);
            NebulaCache.main(new String[] {server.address()});
        } finally {
            System.setOut(originalOutput);
        }

        String result = output.toString(StandardCharsets.UTF_8);
        assertTrue(result.contains("PING: PONG"));
        assertTrue(result.contains("GET: NebulaCache Java SDK smoke test"));
        assertTrue(result.contains("DELETE: true"));
        assertTrue(ENTRIES.isEmpty());
    }

    @Test
    void reportsConnectionAndServerErrors() {
        try (NebulaCache unreachable = new NebulaCache("127.0.0.1:1", Duration.ofMillis(100))) {
            assertThrows(CacheConnectionException.class, unreachable::ping);
        }
        CacheResponseException error = assertThrows(CacheResponseException.class, () -> cache.get("failure"));
        assertEquals(3, error.getStatus());
    }

    @Test
    void configDefaultsAndValidation() {
        NebulaCache.Config defaults = new NebulaCache.Config();
        assertEquals(0, defaults.minPoolSize());
        assertEquals(10, defaults.maxPoolSize());
        assertEquals(Duration.ofSeconds(5), defaults.connectTimeout());
        assertEquals(Duration.ofSeconds(5), defaults.requestTimeout());
        assertEquals(Duration.ofSeconds(5), defaults.acquireTimeout());
        assertEquals(Duration.ofSeconds(60), defaults.idleTimeout());
        assertEquals(Duration.ofSeconds(300), defaults.maxLifetime());
        assertEquals(2, defaults.maxRetries());
        assertFalse(defaults.tlsEnabled());
        NebulaCache.Config plaintextToken = new NebulaCache.Config(
                0, 1, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1),
                Duration.ofSeconds(1), Duration.ofSeconds(1), 0, false, "token");
        assertEquals("token", plaintextToken.token());
        assertThrows(IllegalArgumentException.class, () -> config(-1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> config(0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> config(2, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> config(0, 1, -1));
        for (int index = 0; index < 5; index++) {
            for (Duration invalid : List.of(Duration.ZERO, Duration.ofNanos(-1))) {
                Duration[] durations = new Duration[] {
                    Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1),
                    Duration.ofSeconds(1), Duration.ofSeconds(1)
                };
                durations[index] = invalid;
                assertThrows(IllegalArgumentException.class, () -> new NebulaCache.Config(
                        0, 1, durations[0], durations[1], durations[2], durations[3], durations[4], 0));
            }
        }
    }

    @Test
    void sequentialOperationsReuseOneConnection() throws Exception {
        try (TestServer fixture = new TestServer(NebulaCacheTest::respond);
                NebulaCache client = new NebulaCache(fixture.address(), config(0, 2, 0))) {
            assertTrue(client.ping());
            client.set("reuse", "value");
            assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), client.get("reuse").orElseThrow());
            assertTrue(client.delete("reuse"));
            assertEquals(1, fixture.accepted.get());
            assertEquals(4, fixture.requests.get());
        }
    }

    @Test
    void poolAllowsConcurrentRequestsButBoundsAcquisition() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return respond(request, socket, output);
        }); NebulaCache client = new NebulaCache(fixture.address(), new NebulaCache.Config(
                0, 2, Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofMillis(120),
                Duration.ofSeconds(10), Duration.ofSeconds(20), 0))) {
            Future<Boolean> first = callers.submit(client::ping);
            Future<Boolean> second = callers.submit(client::ping);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            long start = System.nanoTime();
            assertThrows(CacheConnectionException.class, client::ping);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMillis >= 80, "acquisition should wait for its configured deadline");
            assertTrue(elapsedMillis < 1500, "acquisition must not use the longer request timeout");
            assertEquals(2, fixture.accepted.get());
            assertEquals(2, fixture.requests.get());
            release.countDown();
            assertTrue(first.get(2, TimeUnit.SECONDS));
            assertTrue(second.get(2, TimeUnit.SECONDS));
            assertTrue(client.ping());
            assertEquals(2, fixture.accepted.get());
        } finally {
            release.countDown();
            callers.shutdownNow();
        }
    }

    @Test
    void minimumPoolIsReplenishedAfterIdleExpiry() throws Exception {
        assertMinimumPoolReplenished(Duration.ofMillis(80), Duration.ofSeconds(10));
    }

    @Test
    void minimumPoolIsReplenishedAfterLifetimeExpiry() throws Exception {
        assertMinimumPoolReplenished(Duration.ofSeconds(10), Duration.ofMillis(80));
    }

    private void assertMinimumPoolReplenished(Duration idleTimeout, Duration maxLifetime) throws Exception {
        try (TestServer fixture = new TestServer(NebulaCacheTest::respond);
                NebulaCache client = new NebulaCache(fixture.address(), new NebulaCache.Config(
                        2, 2, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1),
                        idleTimeout, maxLifetime, 0))) {
            assertTrue(client.ping());
            await(() -> fixture.accepted.get() == 2);
            Thread.sleep(180);
            assertTrue(client.ping());
            await(() -> fixture.accepted.get() >= 4);
            assertEquals(4, fixture.accepted.get());
        }
    }

    @Test
    void expiredLeasedConnectionIsDiscardedOnRelease() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService callers = Executors.newSingleThreadExecutor();
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            if (entered.getCount() > 0) {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
            }
            return respond(request, socket, output);
        }); NebulaCache client = new NebulaCache(fixture.address(), new NebulaCache.Config(
                0, 1, Duration.ofSeconds(1), Duration.ofSeconds(3), Duration.ofSeconds(1),
                Duration.ofSeconds(10), Duration.ofMillis(80), 0))) {
            Future<Boolean> pending = callers.submit(client::ping);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Thread.sleep(180);
            release.countDown();
            assertTrue(pending.get(2, TimeUnit.SECONDS));
            await(fixture.sockets::isEmpty);
            assertTrue(client.ping());
            assertEquals(2, fixture.accepted.get());
        } finally {
            release.countDown();
            callers.shutdownNow();
        }
    }

    @Test
    void timedOutConnectionIsDiscardedBeforeNextRequest() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            if (attempts.incrementAndGet() == 1) {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            }
            return respond(request, socket, output);
        }); NebulaCache client = new NebulaCache(fixture.address(), new NebulaCache.Config(
                0, 1, Duration.ofSeconds(1), Duration.ofMillis(120), Duration.ofSeconds(1),
                Duration.ofSeconds(10), Duration.ofSeconds(20), 0))) {
            assertThrows(CacheConnectionException.class, client::ping);
            assertTrue(client.ping());
            assertEquals(2, fixture.accepted.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void requestDeadlineCoversEntireResponseRatherThanEachRead() throws Exception {
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            writeResponse(new DataOutputStream(bytes), 0, "PONG".getBytes(StandardCharsets.US_ASCII));
            for (byte value : bytes.toByteArray()) {
                output.writeByte(value);
                output.flush();
                Thread.sleep(40);
            }
            return true;
        }); NebulaCache client = new NebulaCache(fixture.address(), new NebulaCache.Config(
                0, 1, Duration.ofSeconds(1), Duration.ofMillis(150), Duration.ofSeconds(1),
                Duration.ofSeconds(10), Duration.ofSeconds(20), 0))) {
            long start = System.nanoTime();
            assertThrows(CacheConnectionException.class, client::ping);
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 450,
                    "a trickling response must not extend the total wire deadline");
        }
    }

    @Test
    void getRetriesEofOnFreshConnection() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            if (attempts.incrementAndGet() == 1) {
                return false;
            }
            writeResponse(output, 0, "recovered".getBytes(StandardCharsets.UTF_8));
            return true;
        }); NebulaCache client = new NebulaCache(fixture.address(), config(0, 1, 2))) {
            assertArrayEquals("recovered".getBytes(StandardCharsets.UTF_8), client.get("retry").orElseThrow());
            assertEquals(2, attempts.get());
            assertEquals(2, fixture.accepted.get());
        }
    }

    @Test
    void getRetriesResetButMutationsAndPingAreNeverReplayed() throws Exception {
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            socket.setSoLinger(true, 0);
            return false;
        }); NebulaCache client = new NebulaCache(fixture.address(), config(0, 1, 1))) {
            assertThrows(CacheConnectionException.class, () -> client.get("reset"));
            assertEquals(2, fixture.requests.get());
            assertThrows(CacheConnectionException.class, () -> client.set("set", "value"));
            assertEquals(3, fixture.requests.get());
            assertThrows(CacheConnectionException.class, () -> client.delete("delete"));
            assertEquals(4, fixture.requests.get());
            assertThrows(CacheConnectionException.class, client::ping);
            assertEquals(5, fixture.requests.get());
        }
    }

    @Test
    void getRetriesRequestTimeout() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            if (attempts.incrementAndGet() == 1) {
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return false;
            }
            writeResponse(output, 1, new byte[0]);
            return true;
        }); NebulaCache client = new NebulaCache(fixture.address(), new NebulaCache.Config(
                0, 1, Duration.ofSeconds(1), Duration.ofMillis(100), Duration.ofSeconds(1),
                Duration.ofSeconds(10), Duration.ofSeconds(20), 1))) {
            assertTrue(client.get("timeout").isEmpty());
            assertEquals(2, attempts.get());
            assertEquals(2, fixture.accepted.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void protocolAndServerErrorsAreNotRetried() throws Exception {
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            if (request.key().equals("protocol")) {
                output.write(new byte[] {'B', 'A', 'D', '!'});
                output.flush();
            } else {
                writeResponse(output, 3, "forced failure".getBytes(StandardCharsets.UTF_8));
            }
            return true;
        }); NebulaCache client = new NebulaCache(fixture.address(), config(0, 1, 3))) {
            assertThrows(CacheProtocolException.class, () -> client.get("protocol"));
            assertEquals(1, fixture.requests.get());
            CacheResponseException error = assertThrows(CacheResponseException.class, () -> client.get("server"));
            assertEquals(3, error.getStatus());
            assertEquals(2, fixture.requests.get());
            assertEquals(2, fixture.accepted.get());
        }
    }

    @Test
    void getHonorsRetryLimitIncludingZero() throws Exception {
        for (int retries : new int[] {0, 2}) {
            try (TestServer fixture = new TestServer((request, socket, output) -> false);
                    NebulaCache client = new NebulaCache(fixture.address(), config(0, 1, retries))) {
                assertThrows(CacheConnectionException.class, () -> client.get("always-eof"));
                assertEquals(retries + 1, fixture.requests.get());
                assertEquals(retries + 1, fixture.accepted.get());
            }
        }
    }

    @Test
    void concurrentGetsShareClientRetryBudget() throws Exception {
        int operations = 12;
        CountDownLatch firstAttempts = new CountDownLatch(operations);
        AtomicInteger attempts = new AtomicInteger();
        ExecutorService callers = Executors.newFixedThreadPool(operations);
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            if (attempts.incrementAndGet() <= operations) {
                firstAttempts.countDown();
                assertTrue(firstAttempts.await(5, TimeUnit.SECONDS));
            }
            return false;
        }); NebulaCache client = new NebulaCache(fixture.address(), config(0, operations, 1))) {
            long start = System.nanoTime();
            List<Future<?>> pending = new ArrayList<>();
            for (int index = 0; index < operations; index++) {
                pending.add(callers.submit(() -> assertThrows(CacheConnectionException.class,
                        () -> client.get("budget"))));
            }
            for (Future<?> future : pending) {
                future.get(5, TimeUnit.SECONDS);
            }
            long elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
            assertTrue(attempts.get() > operations, "the initial token budget should permit retries");
            assertTrue(attempts.get() <= operations + 10 + elapsedSeconds,
                    "all calls must share the capacity-10, one-token-per-second budget");
            if (elapsedSeconds == 0) {
                assertEquals(operations + 10, attempts.get());
            }
            Thread.sleep(1100);
            int beforeRefill = attempts.get();
            assertThrows(CacheConnectionException.class, () -> client.get("refilled-budget"));
            assertEquals(beforeRefill + 2, attempts.get());
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void closeIsIdempotentClosesIdleSocketsAndRejectsOperations() throws Exception {
        try (TestServer fixture = new TestServer(NebulaCacheTest::respond);
                NebulaCache client = new NebulaCache(fixture.address(), config(0, 1, 0))) {
            assertTrue(client.ping());
            assertEquals(1, fixture.sockets.size());
            client.close();
            client.close();
            await(fixture.sockets::isEmpty);
            assertThrows(IllegalStateException.class, client::ping);
            assertThrows(IllegalStateException.class, () -> client.get("closed"));
            assertThrows(IllegalStateException.class, () -> client.set("closed", "value"));
            assertThrows(IllegalStateException.class, () -> client.delete("closed"));
            assertEquals(1, fixture.accepted.get());
        }
    }

    @Test
    void closeInterruptsLeasedConnectionAndWakesPoolWaiter() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch waiterStarted = new CountDownLatch(1);
        CountDownLatch peerClosed = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try (TestServer fixture = new TestServer((request, socket, output) -> {
            entered.countDown();
            assertEquals(-1, socket.getInputStream().read());
            peerClosed.countDown();
            return false;
        }); NebulaCache client = new NebulaCache(fixture.address(), new NebulaCache.Config(
                0, 1, Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(10), Duration.ofSeconds(20), 2))) {
            Future<Boolean> leased = callers.submit(client::ping);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<Boolean> waiter = callers.submit(() -> {
                waiterStarted.countDown();
                return client.ping();
            });
            assertTrue(waiterStarted.await(2, TimeUnit.SECONDS));
            client.close();
            ExecutionException leasedError = assertThrows(ExecutionException.class,
                    () -> leased.get(1, TimeUnit.SECONDS));
            assertTrue(leasedError.getCause() instanceof CacheConnectionException
                    || leasedError.getCause() instanceof IllegalStateException);
            ExecutionException waiterError = assertThrows(ExecutionException.class,
                    () -> waiter.get(1, TimeUnit.SECONDS));
            assertTrue(waiterError.getCause() instanceof IllegalStateException);
            assertTrue(peerClosed.await(1, TimeUnit.SECONDS));
            await(fixture.sockets::isEmpty);
            assertEquals(1, fixture.accepted.get());
        } finally {
            callers.shutdownNow();
        }
    }

    private static NebulaCache.Config config(int minimum, int maximum, int retries) {
        return new NebulaCache.Config(minimum, maximum, Duration.ofSeconds(1), Duration.ofSeconds(2),
                Duration.ofSeconds(1), Duration.ofSeconds(10), Duration.ofSeconds(20), retries);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true before deadline");
    }

    private static boolean respond(Request request, Socket socket, DataOutputStream output) throws IOException {
        int status = 0;
        byte[] responseValue = new byte[0];
        switch (request.opcode()) {
            case 1 -> ENTRIES.put(request.key(), new Entry(request.value(), request.ttlSecs()));
            case 2 -> {
                if ("failure".equals(request.key())) {
                    status = 3;
                    responseValue = "forced failure".getBytes(StandardCharsets.UTF_8);
                } else {
                    Entry entry = ENTRIES.get(request.key());
                    if (entry == null) {
                        status = 1;
                    } else {
                        responseValue = entry.value();
                    }
                }
            }
            case 3 -> {
                if (ENTRIES.remove(request.key()) == null) {
                    status = 1;
                }
            }
            case 4 -> responseValue = "PONG".getBytes(StandardCharsets.US_ASCII);
            default -> {
                status = 2;
                responseValue = "invalid request".getBytes(StandardCharsets.UTF_8);
            }
        }
        writeResponse(output, status, responseValue);
        return true;
    }

    private static void writeResponse(DataOutputStream output, int status, byte[] value) throws IOException {
        output.write(MAGIC);
        output.writeByte(1);
        output.writeByte(status);
        output.writeInt(value.length);
        output.write(value);
        output.flush();
    }

    @FunctionalInterface
    private interface Handler {
        boolean handle(Request request, Socket socket, DataOutputStream output) throws Exception;
    }

    private static final class TestServer implements AutoCloseable {
        private final ServerSocket listener;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        private final AtomicInteger accepted = new AtomicInteger();
        private final AtomicInteger requests = new AtomicInteger();
        private final Handler handler;
        private final Future<?> acceptor;

        private TestServer(Handler handler) throws IOException {
            this.handler = handler;
            listener = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            acceptor = executor.submit(() -> {
                while (!listener.isClosed()) {
                    try {
                        Socket socket = listener.accept();
                        sockets.add(socket);
                        accepted.incrementAndGet();
                        executor.submit(() -> handleConnection(socket));
                    } catch (IOException error) {
                        if (!listener.isClosed()) {
                            failures.add(error);
                        }
                    }
                }
            });
        }

        private String address() {
            return "127.0.0.1:" + listener.getLocalPort();
        }

        private void handleConnection(Socket socket) {
            try (socket;
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
                while (!listener.isClosed()) {
                    int first = input.read();
                    if (first == -1) {
                        return;
                    }
                    byte[] magic = new byte[MAGIC.length];
                    magic[0] = (byte) first;
                    input.readFully(magic, 1, magic.length - 1);
                    int version = input.readUnsignedByte();
                    int opcode = input.readUnsignedByte();
                    int keyLength = input.readInt();
                    int valueLength = input.readInt();
                    long ttlSecs = input.readLong();
                    assertArrayEquals(MAGIC, magic);
                    assertEquals(1, version);
                    assertTrue(keyLength >= 0 && keyLength <= 1024);
                    assertTrue(valueLength >= 0 && valueLength <= 8 * 1024 * 1024);
                    byte[] keyBytes = new byte[keyLength];
                    byte[] value = new byte[valueLength];
                    input.readFully(keyBytes);
                    input.readFully(value);
                    requests.incrementAndGet();
                    Request request = new Request(opcode, new String(keyBytes, StandardCharsets.UTF_8),
                            value, new BigInteger(Long.toUnsignedString(ttlSecs)));
                    if (!handler.handle(request, socket, output)) {
                        return;
                    }
                }
            } catch (EOFException | SocketException expected) {
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            } catch (Throwable error) {
                failures.add(error);
            } finally {
                sockets.remove(socket);
            }
        }

        @Override
        public void close() throws Exception {
            listener.close();
            acceptor.get(2, TimeUnit.SECONDS);
            for (Socket socket : sockets) {
                socket.close();
            }
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS), "server workers did not terminate");
            assertTrue(failures.isEmpty(), () -> "server failures: " + failures);
        }
    }

    private record Request(int opcode, String key, byte[] value, BigInteger ttlSecs) {}

    private record Entry(byte[] value, BigInteger ttlSecs) {
        private Entry {
            value = value.clone();
        }
    }
}
