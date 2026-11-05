package io.nebulacache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NebulaCacheTest {
    private static final byte[] MAGIC = new byte[] {'N', 'E', 'B', 'U'};
    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();
    private static ServerSocket server;
    private static ExecutorService serverExecutor;
    private static NebulaCache cache;
    private static int port;

    @BeforeAll
    static void startServer() throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        port = server.getLocalPort();
        cache = new NebulaCache("127.0.0.1:" + port);
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            while (!server.isClosed()) {
                try {
                    handleConnection(server.accept());
                } catch (SocketException error) {
                    if (!server.isClosed()) {
                        throw new IllegalStateException(error);
                    }
                } catch (IOException error) {
                    throw new IllegalStateException(error);
                }
            }
        });
    }

    @AfterAll
    static void stopServer() throws IOException {
        server.close();
        serverExecutor.shutdownNow();
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
        assertThrows(IllegalArgumentException.class, () -> new NebulaCache("::1:7001"));
        assertThrows(IllegalArgumentException.class, () -> new NebulaCache("127.0.0.1:7001", Duration.ZERO));
    }

    @Test
    void mainRunsSdkSmokeFlow() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOutput = System.out;
        try (PrintStream capturedOutput = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capturedOutput);
            NebulaCache.main(new String[] {"127.0.0.1:" + port});
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
        NebulaCache unreachable = new NebulaCache("127.0.0.1:1", Duration.ofMillis(100));
        assertThrows(CacheConnectionException.class, unreachable::ping);

        CacheResponseException error = assertThrows(CacheResponseException.class, () -> cache.get("failure"));
        assertEquals(3, error.getStatus());
    }

    private static void handleConnection(Socket socket) throws IOException {
        try (socket;
                DataInputStream input = new DataInputStream(socket.getInputStream());
                DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            int version = input.readUnsignedByte();
            int opcode = input.readUnsignedByte();
            int keyLength = input.readInt();
            int valueLength = input.readInt();
            long ttlSecs = input.readLong();
            byte[] keyBytes = new byte[keyLength];
            byte[] value = new byte[valueLength];
            input.readFully(keyBytes);
            input.readFully(value);

            String key = new String(keyBytes, StandardCharsets.UTF_8);
            int status = 0;
            byte[] responseValue = new byte[0];
            if (!Arrays.equals(magic, MAGIC) || version != 1) {
                return;
            }

            switch (opcode) {
                case 1 -> ENTRIES.put(key, new Entry(value, new BigInteger(Long.toUnsignedString(ttlSecs))));
                case 2 -> {
                    if ("failure".equals(key)) {
                        status = 3;
                        responseValue = "forced failure".getBytes(StandardCharsets.UTF_8);
                    } else if (ENTRIES.containsKey(key)) {
                        responseValue = ENTRIES.get(key).value();
                    } else {
                        status = 1;
                    }
                }
                case 3 -> {
                    if (ENTRIES.remove(key) == null) {
                        status = 1;
                    }
                }
                case 4 -> responseValue = "PONG".getBytes(StandardCharsets.US_ASCII);
                default -> {
                    status = 2;
                    responseValue = "invalid request".getBytes(StandardCharsets.UTF_8);
                }
            }

            output.write(MAGIC);
            output.writeByte(1);
            output.writeByte(status);
            output.writeInt(responseValue.length);
            output.write(responseValue);
            output.flush();
        }
    }

    private record Entry(byte[] value, BigInteger ttlSecs) {
        private Entry {
            value = value.clone();
        }
    }
}
