import inspect
import socket
import socketserver
import struct
import threading
import time
import unittest
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from unittest.mock import patch

from nebulacache import CacheConnectionError, CacheProtocolError, CacheResponseError, NebulaCache

_REQUEST_HEADER = struct.Struct("!4sBBIIQ")
_RESPONSE_HEADER = struct.Struct("!4sBBI")


@dataclass
class ResponsePlan:
    delay: float = 0.0
    entered: threading.Event | None = None
    release: threading.Event | None = None
    disconnect: bool = False
    reset: bool = False
    response: bytes | None = None
    fragments: tuple[tuple[float, bytes], ...] | None = None


class CacheHandler(socketserver.BaseRequestHandler):
    entries: dict[bytes, bytes] = {}
    ttls: dict[bytes, int] = {}

    def handle(self) -> None:
        server = self.server
        with server.condition:
            session = server.session_ids[self.request]
        try:
            while not server.stopping.is_set():
                header = self._receive_exactly(_REQUEST_HEADER.size)
                magic, version, opcode, key_len, value_len, ttl_secs = _REQUEST_HEADER.unpack(header)
                if magic != b"NEBU" or version != 1:
                    return
                key = self._receive_exactly(key_len)
                value = self._receive_exactly(value_len)
                with server.condition:
                    server.requests.append((session, opcode, key, value, ttl_secs))
                    server.active += 1
                    server.peak_active = max(server.peak_active, server.active)
                    plans = server.plans[(opcode, key)]
                    plan = plans.popleft() if plans else ResponsePlan()
                    server.condition.notify_all()
                try:
                    if plan.entered is not None:
                        plan.entered.set()
                    if plan.release is not None:
                        while not plan.release.wait(0.01):
                            if server.stopping.is_set():
                                return
                    if server.stopping.wait(plan.delay):
                        return
                    if plan.disconnect:
                        if plan.reset:
                            self.request.setsockopt(
                                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                            )
                        return
                    with server.condition:
                        if opcode == 4:
                            status, response_value = 0, b"PONG"
                        elif opcode == 1:
                            server.entries[key] = value
                            server.ttls[key] = ttl_secs
                            status, response_value = 0, b""
                        elif opcode == 2 and key == b"failure":
                            status, response_value = 3, b"forced failure"
                        elif opcode == 2 and key in server.entries:
                            status, response_value = 0, server.entries[key]
                        elif opcode == 2:
                            status, response_value = 1, b""
                        elif opcode == 3 and key in server.entries:
                            del server.entries[key]
                            server.ttls.pop(key, None)
                            status, response_value = 0, b""
                        elif opcode == 3:
                            status, response_value = 1, b""
                        else:
                            status, response_value = 2, b"invalid request"
                    response = _RESPONSE_HEADER.pack(b"NEBU", 1, status, len(response_value))
                    response += response_value
                    if plan.response is not None:
                        response = plan.response
                    if plan.fragments is None:
                        self.request.sendall(response)
                    else:
                        for delay, fragment in plan.fragments:
                            if server.stopping.wait(delay):
                                return
                            self.request.sendall(fragment)
                        return
                finally:
                    with server.condition:
                        server.active -= 1
                        server.condition.notify_all()
        except (OSError, ConnectionError):
            pass
        finally:
            with server.condition:
                server.sockets.discard(self.request)
                server.condition.notify_all()

    def _receive_exactly(self, length: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < length:
            chunk = self.request.recv(length - len(chunks))
            if not chunk:
                raise ConnectionError("client disconnected before request was complete")
            chunks.extend(chunk)
        return bytes(chunks)


class CacheServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.stopping = threading.Event()
        self.entries = {}
        self.ttls = {}
        self.plans = defaultdict(deque)
        self.requests = []
        self.session_ids = {}
        self.sockets = set()
        self.accepted = 0
        self.active = 0
        self.peak_active = 0
        super().__init__(("127.0.0.1", 0), CacheHandler)

    def get_request(self):
        connection, address = super().get_request()
        with self.condition:
            self.accepted += 1
            self.session_ids[connection] = self.accepted
            self.sockets.add(connection)
            self.condition.notify_all()
        return connection, address

    def plan(self, opcode: int, key: bytes = b"", **options) -> ResponsePlan:
        plan = ResponsePlan(**options)
        with self.condition:
            self.plans[(opcode, key)].append(plan)
        return plan

    def count(self, opcode: int | None = None, key: bytes | None = None) -> int:
        with self.condition:
            return sum(
                (opcode is None or request[1] == opcode)
                and (key is None or request[2] == key)
                for request in self.requests
            )

    def wait_for(self, predicate, timeout: float = 2.0) -> bool:
        with self.condition:
            return self.condition.wait_for(predicate, timeout)

    def stop(self) -> None:
        self.stopping.set()
        with self.condition:
            sockets = tuple(self.sockets)
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        self.shutdown()
        self.server_close()


class NebulaCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = CacheServer()
        CacheHandler.entries = self.server.entries
        CacheHandler.ttls = self.server.ttls
        self.server_thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        self.server_thread.start()
        self.addCleanup(self.server_thread.join, 2)
        self.addCleanup(self.server.stop)
        self.address = f"127.0.0.1:{self.server.server_address[1]}"
        self.cache = self.make_cache()

    def make_cache(self, *args, **options) -> NebulaCache:
        cache = NebulaCache(self.address, *args, **options)
        close = getattr(cache, "close", None)
        if close is not None:
            self.addCleanup(close)
        return cache

    def seed(self, key: bytes = b"key", value: bytes = b"value") -> None:
        with self.server.condition:
            self.server.entries[key] = value

    def pool_sockets(self, cache: NebulaCache) -> tuple[socket.socket, ...]:
        with cache._condition:
            return tuple(connection.socket for connection in cache._connections)

    def test_ping_and_key_lifecycle(self) -> None:
        self.assertTrue(self.cache.ping())
        self.cache.set("greeting", "hello", ttl_secs=30)
        self.assertEqual(self.cache.get("greeting"), b"hello")
        with self.server.condition:
            self.assertEqual(CacheHandler.ttls[b"greeting"], 30)
        self.assertTrue(self.cache.delete("greeting"))
        self.assertFalse(self.cache.delete("greeting"))
        self.assertIsNone(self.cache.get("greeting"))

    def test_binary_values_and_utf8_keys(self) -> None:
        key = "space / café"
        value = b"\x00\xff"
        self.cache.set(key, value)
        self.assertEqual(self.cache.get(key), value)
        with self.server.condition:
            self.assertEqual(CacheHandler.ttls[key.encode("utf-8")], 0)

    def test_empty_binary_value_is_not_missing(self) -> None:
        self.cache.set("empty", b"")
        self.assertEqual(self.cache.get("empty"), b"")

    def test_invalid_arguments_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.cache.set("", "value")
        with self.assertRaises(ValueError):
            self.cache.set("key", "value", ttl_secs=-1)
        with self.assertRaises(TypeError):
            self.cache.set("key", 1)
        with self.assertRaises(ValueError):
            self.cache.set("é" * 513, "value")
        with self.assertRaises(ValueError):
            NebulaCache("localhost:70000")
        with self.assertRaises(TypeError):
            NebulaCache(ssl_context=object())

    def test_unreachable_node_raises_connection_error(self) -> None:
        with self.assertRaises(CacheConnectionError):
            NebulaCache("127.0.0.1:1", timeout=0.1).ping()

    def test_server_error_exposes_protocol_status(self) -> None:
        with self.assertRaises(CacheResponseError) as raised:
            self.cache.get("failure")
        self.assertEqual(raised.exception.status, 3)

    def test_pool_option_defaults_and_keyword_only_options(self) -> None:
        parameters = inspect.signature(NebulaCache).parameters
        expected = {
            "connect_timeout": None,
            "request_timeout": None,
            "min_pool_size": 0,
            "max_pool_size": 10,
            "acquire_timeout": 5.0,
            "idle_timeout": 60.0,
            "max_lifetime": 300.0,
            "max_retries": 2,
        }
        for name, default in expected.items():
            with self.subTest(option=name):
                self.assertIn(name, parameters)
                self.assertEqual(parameters[name].default, default)
                self.assertEqual(parameters[name].kind, inspect.Parameter.KEYWORD_ONLY)

    def test_timeout_inheritance_and_explicit_connect_override(self) -> None:
        for timeout, connect_timeout, request_timeout, expected in (
            (None, None, None, 5.0),
            (0.7, None, None, 0.7),
            (0.7, 0.2, 0.3, 0.2),
        ):
            with self.subTest(timeout=timeout, connect_timeout=connect_timeout):
                args = () if timeout is None else (timeout,)
                cache = self.make_cache(
                    *args, connect_timeout=connect_timeout, request_timeout=request_timeout
                )
                with patch(
                    "nebulacache.client.socket.create_connection", wraps=socket.create_connection
                ) as connect:
                    self.assertTrue(cache.ping())
                self.assertTrue(connect.called)
                for call in connect.call_args_list:
                    actual = call.kwargs.get("timeout")
                    if actual is None and len(call.args) > 1:
                        actual = call.args[1]
                    self.assertIsNotNone(actual)
                    self.assertAlmostEqual(actual, expected, delta=0.03)

    def test_pool_configuration_validation(self) -> None:
        for options in (
            {"min_pool_size": -1},
            {"max_pool_size": 0},
            {"min_pool_size": 3, "max_pool_size": 2},
            {"min_pool_size": 0.5},
            {"max_pool_size": 1.5},
            {"connect_timeout": 0},
            {"request_timeout": -1},
            {"acquire_timeout": 0},
            {"idle_timeout": -1},
            {"max_lifetime": -1},
            {"max_retries": -1},
            {"max_retries": 1.5},
        ):
            with self.subTest(options=options):
                with self.assertRaises((ValueError, TypeError)):
                    self.make_cache(**options)

    def test_sequential_operations_reuse_one_session(self) -> None:
        for index in range(5):
            self.cache.set(f"key-{index}", f"value-{index}")
            self.assertEqual(self.cache.get(f"key-{index}"), f"value-{index}".encode())
            self.assertTrue(self.cache.delete(f"key-{index}"))
            self.assertTrue(self.cache.ping())
        with self.server.condition:
            self.assertEqual(self.server.accepted, 1)
            self.assertEqual({request[0] for request in self.server.requests}, {1})

    def test_minimum_pool_target_is_lazy(self) -> None:
        cache = self.make_cache(min_pool_size=3, max_pool_size=4)
        with self.server.condition:
            self.assertEqual(self.server.accepted, 0)
        self.assertTrue(cache.ping())
        self.assertTrue(self.server.wait_for(lambda: self.server.accepted >= 3))
        with cache._condition:
            self.assertGreaterEqual(len(cache._connections), 3)
            self.assertLessEqual(len(cache._connections), 4)
        for _ in range(8):
            self.assertTrue(cache.ping())
        with self.server.condition:
            self.assertLessEqual(self.server.accepted, 4)

    def test_concurrent_calls_respect_cap_and_do_not_mix_responses(self) -> None:
        cache = self.make_cache(max_pool_size=3, acquire_timeout=3, request_timeout=2)
        release = threading.Event()
        self.addCleanup(release.set)
        for index in range(18):
            key = f"key-{index}".encode()
            self.seed(key, b"value-" + key)
            self.server.plan(2, key, release=release)
        with ThreadPoolExecutor(max_workers=18) as executor:
            futures = [executor.submit(cache.get, f"key-{index}") for index in range(18)]
            try:
                self.assertTrue(self.server.wait_for(lambda: self.server.active == 3))
                with self.server.condition:
                    self.assertEqual(self.server.accepted, 3)
                with cache._condition:
                    self.assertLessEqual(len(cache._connections), 3)
            finally:
                release.set()
            for index, future in enumerate(futures):
                self.assertEqual(future.result(timeout=3), f"value-key-{index}".encode())
        with self.server.condition:
            self.assertLessEqual(self.server.peak_active, 3)
            self.assertEqual(self.server.accepted, 3)

    def test_acquisition_timeout_does_not_steal_active_connection(self) -> None:
        cache = self.make_cache(max_pool_size=1, acquire_timeout=0.1, request_timeout=2)
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        self.server.plan(4, entered=entered, release=release)
        with ThreadPoolExecutor(max_workers=1) as executor:
            active = executor.submit(cache.ping)
            try:
                self.assertTrue(entered.wait(1))
                started = time.monotonic()
                with self.assertRaises(CacheConnectionError):
                    cache.ping()
                self.assertLess(time.monotonic() - started, 0.7)
                self.assertEqual(self.server.count(), 1)
            finally:
                release.set()
            self.assertTrue(active.result(timeout=2))
        self.assertTrue(cache.ping())
        with self.server.condition:
            self.assertEqual(self.server.accepted, 1)

    def test_connect_timeout_is_transport_error_and_releases_capacity(self) -> None:
        cache = self.make_cache(connect_timeout=0.05, max_pool_size=1, max_retries=0)
        with patch(
            "nebulacache.client.socket.create_connection", side_effect=socket.timeout("connect")
        ) as connect:
            with self.assertRaises(CacheConnectionError):
                cache.ping()
            self.assertEqual(connect.call_count, 1)
        self.assertTrue(cache.ping())

    def test_request_timeout_inherits_positional_timeout(self) -> None:
        cache = self.make_cache(0.08, max_retries=0)
        self.server.plan(4, delay=0.3)
        started = time.monotonic()
        with self.assertRaises(CacheConnectionError):
            cache.ping()
        self.assertLess(time.monotonic() - started, 0.25)
        self.assertTrue(cache.ping())

    def test_explicit_request_timeout_overrides_legacy_timeout(self) -> None:
        cache = self.make_cache(0.04, request_timeout=0.5, max_retries=0)
        self.server.plan(4, delay=0.1)
        self.assertTrue(cache.ping())
        cache = self.make_cache(1, request_timeout=0.06, max_retries=0)
        self.server.plan(4, delay=0.3)
        with self.assertRaises(CacheConnectionError):
            cache.ping()

    def test_request_timeout_is_total_across_header_and_body_fragments(self) -> None:
        cache = self.make_cache(request_timeout=0.18, max_retries=0)
        header = _RESPONSE_HEADER.pack(b"NEBU", 1, 0, 4)
        self.server.plan(
            4,
            fragments=((0.07, header[:5]), (0.07, header[5:]), (0.07, b"PO"), (0.07, b"NG")),
        )
        started = time.monotonic()
        with self.assertRaises(CacheConnectionError):
            cache.ping()
        self.assertLess(time.monotonic() - started, 0.4)
        self.assertTrue(cache.ping())

    def test_request_deadline_includes_send_time(self) -> None:
        cache = self.make_cache(request_timeout=0.2, max_retries=0)
        create_connection = socket.create_connection

        class SlowSendSocket:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def sendall(self, data):
                time.sleep(0.12)
                self.connection.sendall(data)

        def connect(*args, **kwargs):
            return SlowSendSocket(create_connection(*args, **kwargs))

        self.server.plan(4, delay=0.12)
        with patch("nebulacache.client.socket.create_connection", side_effect=connect):
            with self.assertRaises(CacheConnectionError):
                cache.ping()

    def test_protocol_errors_discard_connection_without_retry(self) -> None:
        for response in (
            _RESPONSE_HEADER.pack(b"NOPE", 1, 0, 0),
            _RESPONSE_HEADER.pack(b"NEBU", 2, 0, 0),
            _RESPONSE_HEADER.pack(b"NEBU", 1, 255, 0),
            _RESPONSE_HEADER.pack(b"NEBU", 1, 0, 8 * 1024 * 1024 + 1),
        ):
            with self.subTest(response=response):
                cache = self.make_cache(max_pool_size=1)
                self.seed()
                self.server.plan(2, b"key", response=response)
                before = self.server.count(2, b"key")
                with self.assertRaises(CacheProtocolError):
                    cache.get("key")
                self.assertEqual(self.server.count(2, b"key"), before + 1)
                self.assertEqual(self.pool_sockets(cache), ())
                self.assertEqual(cache.get("key"), b"value")
                with self.server.condition:
                    sessions = [r[0] for r in self.server.requests if r[1:3] == (2, b"key")]
                    self.assertNotEqual(sessions[-1], sessions[-2])
                cache.close()

    def test_invalid_pong_discards_connection_without_retry(self) -> None:
        self.server.plan(4, response=_RESPONSE_HEADER.pack(b"NEBU", 1, 0, 4) + b"NOPE")
        with self.assertRaises(CacheProtocolError):
            self.cache.ping()
        self.assertEqual(self.server.count(4), 1)
        self.assertEqual(self.pool_sockets(self.cache), ())
        self.assertTrue(self.cache.ping())
        with self.server.condition:
            self.assertEqual(self.server.accepted, 2)

    def test_transport_failures_discard_connections(self) -> None:
        for options in ({"disconnect": True}, {"disconnect": True, "reset": True}, {"delay": 0.3}):
            with self.subTest(options=options):
                cache = self.make_cache(request_timeout=0.06, max_retries=0, max_pool_size=1)
                self.server.plan(4, **options)
                with self.assertRaises(CacheConnectionError):
                    cache.ping()
                self.assertEqual(self.pool_sockets(cache), ())
                self.assertTrue(cache.ping())
                cache.close()

    def test_idle_timeout_replaces_expired_connection(self) -> None:
        cache = self.make_cache(idle_timeout=0.08, max_lifetime=5, max_pool_size=1)
        self.assertTrue(cache.ping())
        original = self.pool_sockets(cache)
        self.assertEqual(len(original), 1)
        time.sleep(0.12)
        self.assertTrue(cache.ping())
        self.assertEqual(original[0].fileno(), -1)
        with self.server.condition:
            self.assertEqual(self.server.accepted, 2)

    def test_lifetime_expires_even_when_connection_is_used_regularly(self) -> None:
        cache = self.make_cache(idle_timeout=5, max_lifetime=0.15, max_pool_size=1)
        self.assertTrue(cache.ping())
        original = self.pool_sockets(cache)[0]
        time.sleep(0.06)
        self.assertTrue(cache.ping())
        time.sleep(0.06)
        self.assertTrue(cache.ping())
        time.sleep(0.06)
        self.assertTrue(cache.ping())
        self.assertEqual(original.fileno(), -1)
        with self.server.condition:
            self.assertEqual(self.server.accepted, 2)

    def test_active_request_is_not_interrupted_by_pool_expiry(self) -> None:
        cache = self.make_cache(idle_timeout=0.04, max_lifetime=0.04, request_timeout=1)
        self.server.plan(4, delay=0.12)
        self.assertTrue(cache.ping())
        self.assertTrue(cache.ping())
        with self.server.condition:
            self.assertEqual(self.server.accepted, 2)

    def test_get_retries_eof_reset_partial_frames_and_timeout(self) -> None:
        for options in (
            {"disconnect": True},
            {"disconnect": True, "reset": True},
            {"fragments": ((0, b"NEB"),)},
            {"fragments": ((0, _RESPONSE_HEADER.pack(b"NEBU", 1, 0, 5) + b"va"),)},
            {"delay": 0.2},
        ):
            with self.subTest(options=options):
                cache = self.make_cache(request_timeout=0.06, max_retries=1)
                self.seed()
                self.server.plan(2, b"key", **options)
                before = self.server.count(2, b"key")
                with patch("nebulacache.client.random.uniform", return_value=0):
                    self.assertEqual(cache.get("key"), b"value")
                self.assertEqual(self.server.count(2, b"key"), before + 2)
                with self.server.condition:
                    sessions = [r[0] for r in self.server.requests if r[1:3] == (2, b"key")]
                    self.assertNotEqual(sessions[-1], sessions[-2])
                cache.close()

    def test_get_retries_connect_errors(self) -> None:
        cache = self.make_cache(max_retries=1)
        self.seed()
        connect = socket.create_connection
        calls = []

        def flaky_connect(*args, **kwargs):
            calls.append(None)
            if len(calls) == 1:
                raise socket.timeout("connect")
            return connect(*args, **kwargs)

        with patch("nebulacache.client.socket.create_connection", side_effect=flaky_connect):
            with patch("nebulacache.client.random.uniform", return_value=0):
                self.assertEqual(cache.get("key"), b"value")
        self.assertEqual(len(calls), 2)

    def test_max_retries_limits_attempts(self) -> None:
        for retries in (0, 1, 2, 4):
            with self.subTest(retries=retries):
                cache = self.make_cache(max_retries=retries)
                key = f"fail-{retries}".encode()
                for _ in range(retries + 2):
                    self.server.plan(2, key, disconnect=True)
                with patch("nebulacache.client.random.uniform", return_value=0):
                    with self.assertRaises(CacheConnectionError):
                        cache.get(key.decode())
                self.assertEqual(self.server.count(2, key), retries + 1)

    def test_set_delete_and_ping_never_retry_transport_errors(self) -> None:
        for opcode, key, operation in (
            (1, b"key", lambda cache: cache.set("key", "value")),
            (3, b"key", lambda cache: cache.delete("key")),
            (4, b"", lambda cache: cache.ping()),
        ):
            for failure in ({"disconnect": True}, {"delay": 0.2}):
                with self.subTest(opcode=opcode, failure=failure):
                    cache = self.make_cache(request_timeout=0.05, max_retries=2)
                    self.server.plan(opcode, key, **failure)
                    before = self.server.count(opcode, key)
                    with self.assertRaises(CacheConnectionError):
                        operation(cache)
                    self.assertEqual(self.server.count(opcode, key), before + 1)

    def test_server_errors_are_not_retried_and_connection_stays_healthy(self) -> None:
        for status in (2, 3):
            with self.subTest(status=status):
                cache = self.make_cache(max_retries=2)
                self.server.plan(
                    2, b"key", response=_RESPONSE_HEADER.pack(b"NEBU", 1, status, 5) + b"error"
                )
                before = self.server.count(2, b"key")
                with self.assertRaises(CacheResponseError) as raised:
                    cache.get("key")
                self.assertEqual(raised.exception.status, status)
                self.assertEqual(self.server.count(2, b"key"), before + 1)
                self.assertTrue(cache.ping())
                with self.server.condition:
                    self.assertEqual(self.server.requests[-1][0], self.server.requests[-2][0])
                cache.close()

    def test_missing_get_does_not_retry(self) -> None:
        self.assertIsNone(self.cache.get("missing"))
        self.assertEqual(self.server.count(2, b"missing"), 1)

    def test_retry_budget_is_shared_across_calls_and_refills_one_token_per_second(self) -> None:
        cache = self.make_cache(max_retries=2, idle_timeout=60, max_lifetime=300)
        monotonic = time.monotonic
        offset = [0.0]
        with patch("nebulacache.client.time.monotonic", side_effect=lambda: monotonic() + offset[0]):
            with patch("nebulacache.client.random.uniform", return_value=0):
                for index in range(5):
                    key = f"drain-{index}".encode()
                    for _ in range(4):
                        self.server.plan(2, key, disconnect=True)
                    with self.assertRaises(CacheConnectionError):
                        cache.get(key.decode())
                    self.assertEqual(self.server.count(2, key), 3)
                for _ in range(4):
                    self.server.plan(2, b"empty", disconnect=True)
                with self.assertRaises(CacheConnectionError):
                    cache.get("empty")
                self.assertEqual(self.server.count(2, b"empty"), 1)
                offset[0] += 1.1
                for _ in range(4):
                    self.server.plan(2, b"refilled", disconnect=True)
                with self.assertRaises(CacheConnectionError):
                    cache.get("refilled")
                self.assertEqual(self.server.count(2, b"refilled"), 2)
                offset[0] += 20
                for _ in range(5):
                    self.server.plan(2, b"capped", disconnect=True)
                with self.assertRaises(CacheConnectionError):
                    cache.get("capped")
                self.assertEqual(self.server.count(2, b"capped"), 3)

    def test_retry_budgets_are_independent_between_clients(self) -> None:
        with patch("nebulacache.client.random.uniform", return_value=0):
            for index in range(2):
                cache = self.make_cache(max_retries=1)
                key = f"client-{index}".encode()
                self.seed(key, b"ok")
                self.server.plan(2, key, disconnect=True)
                self.assertEqual(cache.get(key.decode()), b"ok")
                self.assertEqual(self.server.count(2, key), 2)

    def test_concurrent_calls_share_retry_budget(self) -> None:
        cache = self.make_cache(max_retries=2, max_pool_size=4)
        release = threading.Event()
        self.addCleanup(release.set)
        for index in range(4):
            key = f"budget-{index}".encode()
            self.server.plan(2, key, release=release, disconnect=True)
            for _ in range(3):
                self.server.plan(2, key, disconnect=True)
        with patch("nebulacache.client.random.uniform", return_value=0):
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(cache.get, f"budget-{index}") for index in range(4)]
                try:
                    self.assertTrue(self.server.wait_for(lambda: self.server.active == 4))
                finally:
                    release.set()
                for future in futures:
                    with self.assertRaises(CacheConnectionError):
                        future.result(timeout=2)
        self.assertEqual(self.server.count(2), 12)

    def test_retry_uses_capped_exponential_full_jitter(self) -> None:
        cache = self.make_cache(max_retries=7)
        for _ in range(8):
            self.server.plan(2, b"key", disconnect=True)
        with patch("nebulacache.client.random.uniform", return_value=0) as uniform:
            with self.assertRaises(CacheConnectionError):
                cache.get("key")
        self.assertEqual(uniform.call_count, 7)
        for attempt, call in enumerate(uniform.call_args_list):
            self.assertEqual(call.args[0], 0)
            self.assertAlmostEqual(call.args[1], min(1.0, 0.05 * 2**attempt))

    def test_close_is_idempotent_and_later_operations_fail_without_connecting(self) -> None:
        self.cache.close()
        self.cache.close()
        with patch("nebulacache.client.socket.create_connection") as connect:
            for operation in (
                lambda: self.cache.ping(),
                lambda: self.cache.get("key"),
                lambda: self.cache.set("key", "value"),
                lambda: self.cache.delete("key"),
            ):
                with self.subTest(operation=operation):
                    with self.assertRaises(CacheConnectionError):
                        operation()
            connect.assert_not_called()

    def test_close_closes_all_idle_sockets(self) -> None:
        cache = self.make_cache(min_pool_size=3, max_pool_size=3)
        self.assertTrue(cache.ping())
        self.assertTrue(self.server.wait_for(lambda: self.server.accepted == 3))
        sockets = self.pool_sockets(cache)
        self.assertEqual(len(sockets), 3)
        cache.close()
        self.assertTrue(all(connection.fileno() == -1 for connection in sockets))
        with cache._condition:
            self.assertFalse(cache._connections)
            self.assertFalse(cache._idle)
        self.assertTrue(self.server.wait_for(lambda: not self.server.sockets))

    def test_close_interrupts_active_request_and_pool_waiter(self) -> None:
        cache = self.make_cache(max_pool_size=1, acquire_timeout=5, request_timeout=5)
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        self.addCleanup(release.set)
        self.server.plan(2, b"blocked", entered=entered, release=release)

        def wait_for_pool():
            waiting.set()
            return cache.ping()

        with ThreadPoolExecutor(max_workers=2) as executor:
            active = executor.submit(cache.get, "blocked")
            try:
                self.assertTrue(entered.wait(1))
                sockets = self.pool_sockets(cache)
                waiter = executor.submit(wait_for_pool)
                self.assertTrue(waiting.wait(1))
                time.sleep(0.03)
                self.assertFalse(waiter.done())
                started = time.monotonic()
                cache.close()
                for future in (active, waiter):
                    with self.assertRaises(CacheConnectionError):
                        future.result(timeout=0.8)
                self.assertLess(time.monotonic() - started, 0.8)
                self.assertTrue(all(connection.fileno() == -1 for connection in sockets))
                self.assertEqual(self.server.count(), 1)
            finally:
                cache.close()
                release.set()

    def test_close_interrupts_retry_backoff_without_another_attempt(self) -> None:
        cache = self.make_cache(max_retries=2)
        entered = threading.Event()
        self.server.plan(2, b"key", disconnect=True)

        def jitter(*args):
            entered.set()
            return 1.0

        with patch("nebulacache.client.random.uniform", side_effect=jitter):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(cache.get, "key")
                try:
                    self.assertTrue(entered.wait(1))
                    time.sleep(0.03)
                    self.assertFalse(future.done())
                    started = time.monotonic()
                    cache.close()
                    with self.assertRaises(CacheConnectionError):
                        future.result(timeout=0.5)
                    self.assertLess(time.monotonic() - started, 0.5)
                    self.assertEqual(self.server.count(2, b"key"), 1)
                finally:
                    cache.close()


if __name__ == "__main__":
    unittest.main()
