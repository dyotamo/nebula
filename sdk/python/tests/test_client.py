import socketserver
import struct
import threading
import unittest

from nebulacache import CacheConnectionError, CacheResponseError, NebulaCache

_REQUEST_HEADER = struct.Struct("!4sBBIIQ")
_RESPONSE_HEADER = struct.Struct("!4sBBI")


class CacheHandler(socketserver.BaseRequestHandler):
    entries: dict[bytes, bytes] = {}
    ttls: dict[bytes, int] = {}

    def handle(self) -> None:
        header = self._receive_exactly(_REQUEST_HEADER.size)
        magic, version, opcode, key_len, value_len, ttl_secs = _REQUEST_HEADER.unpack(header)
        if magic != b"NEBU" or version != 1:
            return
        key = self._receive_exactly(key_len)
        value = self._receive_exactly(value_len)

        if opcode == 4:
            status, response_value = 0, b"PONG"
        elif opcode == 1:
            self.entries[key] = value
            self.ttls[key] = ttl_secs
            status, response_value = 0, b""
        elif opcode == 2 and key == b"failure":
            status, response_value = 3, b"forced failure"
        elif opcode == 2 and key in self.entries:
            status, response_value = 0, self.entries[key]
        elif opcode == 2:
            status, response_value = 1, b""
        elif opcode == 3 and key in self.entries:
            del self.entries[key]
            status, response_value = 0, b""
        elif opcode == 3:
            status, response_value = 1, b""
        else:
            status, response_value = 2, b"invalid request"

        response = _RESPONSE_HEADER.pack(b"NEBU", 1, status, len(response_value))
        self.request.sendall(response + response_value)

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


class NebulaCacheTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        CacheHandler.entries = {}
        CacheHandler.ttls = {}
        cls.server = CacheServer(("127.0.0.1", 0), CacheHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.address = f"127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join()

    def setUp(self) -> None:
        CacheHandler.entries.clear()
        CacheHandler.ttls.clear()
        self.cache = NebulaCache(self.address)

    def test_ping_and_key_lifecycle(self) -> None:
        self.assertTrue(self.cache.ping())
        self.cache.set("greeting", "hello", ttl_secs=30)
        self.assertEqual(self.cache.get("greeting"), b"hello")
        self.assertEqual(CacheHandler.ttls[b"greeting"], 30)
        self.assertTrue(self.cache.delete("greeting"))
        self.assertFalse(self.cache.delete("greeting"))
        self.assertIsNone(self.cache.get("greeting"))

    def test_binary_values_and_utf8_keys(self) -> None:
        key = "space / café"
        value = b"\x00\xff"
        self.cache.set(key, value)
        self.assertEqual(self.cache.get(key), value)
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

    def test_unreachable_node_raises_connection_error(self) -> None:
        with self.assertRaises(CacheConnectionError):
            NebulaCache("127.0.0.1:1", timeout=0.1).ping()

    def test_server_error_exposes_protocol_status(self) -> None:
        with self.assertRaises(CacheResponseError) as raised:
            self.cache.get("failure")
        self.assertEqual(raised.exception.status, 3)


if __name__ == "__main__":
    unittest.main()
