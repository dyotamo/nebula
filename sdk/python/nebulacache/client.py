from __future__ import annotations

import errno
import math
import random
import socket
import ssl
import struct
import threading
import time
from dataclasses import dataclass
from typing import TypeAlias

from .exceptions import CacheConnectionError, CacheProtocolError, CacheResponseError

CacheValue: TypeAlias = str | bytes | bytearray | memoryview

_REQUEST_HEADER = struct.Struct("!4sBBIIQ")
_RESPONSE_HEADER = struct.Struct("!4sBBI")
_MAGIC = b"NEBU"
_VERSION = 1
_MAX_KEY_LEN = 1024
_MAX_VALUE_LEN = 8 * 1024 * 1024
_STATUS_NAMES = {
    0: "OK",
    1: "NOT_FOUND",
    2: "INVALID_ARGUMENT",
    3: "INTERNAL_ERROR",
    4: "UNAUTHORIZED",
    5: "FORBIDDEN",
}


@dataclass(eq=False)
class _PooledConnection:
    socket: socket.socket
    created_at: float
    last_used: float


class NebulaCache:
    """Synchronous client for the NebulaCache binary TCP protocol."""

    def __init__(
        self,
        address: str = "127.0.0.1:7001",
        timeout: float = 5.0,
        *,
        min_pool_size: int = 0,
        max_pool_size: int = 10,
        connect_timeout: float | None = None,
        request_timeout: float | None = None,
        acquire_timeout: float = 5.0,
        idle_timeout: float = 60.0,
        max_lifetime: float = 300.0,
        max_retries: int = 2,
        ssl_context: ssl.SSLContext | None = None,
        token: str | None = None,
    ) -> None:
        self.host, self.port = self._parse_address(address)
        connect_timeout = timeout if connect_timeout is None else connect_timeout
        request_timeout = timeout if request_timeout is None else request_timeout
        for name, duration in (
            ("timeout", timeout),
            ("connect_timeout", connect_timeout),
            ("request_timeout", request_timeout),
            ("acquire_timeout", acquire_timeout),
            ("idle_timeout", idle_timeout),
            ("max_lifetime", max_lifetime),
        ):
            if (
                isinstance(duration, bool)
                or not isinstance(duration, (int, float))
                or not math.isfinite(duration)
                or duration <= 0
            ):
                raise ValueError(f"{name} must be finite and greater than zero")
        for name, size in (
            ("min_pool_size", min_pool_size),
            ("max_pool_size", max_pool_size),
            ("max_retries", max_retries),
        ):
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if max_pool_size == 0 or min_pool_size > max_pool_size:
            raise ValueError("pool sizes must satisfy 0 <= min_pool_size <= max_pool_size and max_pool_size > 0")
        self.address = address
        self.timeout = timeout
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.acquire_timeout = acquire_timeout
        self.idle_timeout = idle_timeout
        self.max_lifetime = max_lifetime
        self.max_retries = max_retries
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise TypeError("ssl_context must be an ssl.SSLContext or None")
        if token is not None:
            if not isinstance(token, str) or not token or len(token.encode("utf-8")) > 1024:
                raise ValueError("token must be a non-empty string no longer than 1024 bytes")
            if ssl_context is None or ssl_context.verify_mode == ssl.CERT_NONE or not ssl_context.check_hostname:
                raise ValueError("token authentication requires TLS with certificate verification enabled")
        self.ssl_context = ssl_context
        self.token = token
        self._condition = threading.Condition()
        self._closed = threading.Event()
        self._connections: set[_PooledConnection] = set()
        self._idle: list[_PooledConnection] = []
        self._connecting = 0
        self._retry_tokens = 10.0
        self._retry_updated_at = time.monotonic()

    def close(self) -> None:
        with self._condition:
            self._closed.set()
            for connection in self._connections:
                self._close_socket(connection.socket)
            self._connections.clear()
            self._idle.clear()
            self._condition.notify_all()

    @staticmethod
    def _close_socket(connection: socket.socket) -> None:
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        connection.close()

    def _expire_idle(self, now: float) -> None:
        for connection in self._idle[:]:
            if (
                now - connection.last_used >= self.idle_timeout
                or now - connection.created_at >= self.max_lifetime
            ):
                self._idle.remove(connection)
                self._connections.remove(connection)
                self._close_socket(connection.socket)

    def _acquire(self) -> _PooledConnection:
        deadline = time.monotonic() + self.acquire_timeout
        while True:
            with self._condition:
                if self._closed.is_set():
                    raise CacheConnectionError("NebulaCache client is closed")
                now = time.monotonic()
                for connection in self._idle[:]:
                    if (
                        now - connection.last_used >= self.idle_timeout
                        or now - connection.created_at >= self.max_lifetime
                    ):
                        self._idle.remove(connection)
                        self._connections.remove(connection)
                        self._close_socket(connection.socket)
                remaining = deadline - now
                if remaining <= 0:
                    raise CacheConnectionError("Timed out acquiring a NebulaCache connection")
                total = len(self._connections) + self._connecting
                if total < self.min_pool_size or (not self._idle and total < self.max_pool_size):
                    self._connecting += 1
                elif self._idle:
                    return self._idle.pop()
                else:
                    self._condition.wait(remaining)
                    continue
            connection_socket = None
            try:
                connection_socket = socket.create_connection(
                    (self.host, self.port), timeout=min(self.connect_timeout, remaining)
                )
                if self.ssl_context is not None:
                    connection_socket = self.ssl_context.wrap_socket(
                        connection_socket, server_hostname=self.host
                    )
                if self.token is not None:
                    self._authenticate(connection_socket)
            except BaseException:
                if connection_socket is not None:
                    self._close_socket(connection_socket)
                with self._condition:
                    self._connecting -= 1
                    self._condition.notify_all()
                raise
            with self._condition:
                self._connecting -= 1
                if self._closed.is_set():
                    self._close_socket(connection_socket)
                    self._condition.notify_all()
                    raise CacheConnectionError("NebulaCache client is closed")
                now = time.monotonic()
                connection = _PooledConnection(connection_socket, now, now)
                self._connections.add(connection)
                self._idle.append(connection)
                self._condition.notify_all()

    def _release(self, connection: _PooledConnection, healthy: bool) -> None:
        with self._condition:
            if connection not in self._connections:
                return
            now = time.monotonic()
            if not healthy or self._closed.is_set() or now - connection.created_at >= self.max_lifetime:
                self._connections.remove(connection)
                self._close_socket(connection.socket)
            else:
                connection.last_used = now
                self._idle.append(connection)
            self._condition.notify_all()

    def _authenticate(self, connection: socket.socket) -> None:
        token = self.token.encode("utf-8")
        header = _REQUEST_HEADER.pack(_MAGIC, _VERSION, 5, 0, len(token), 0)
        connection.settimeout(self.request_timeout)
        connection.sendall(header + token)
        status, value = self._read_response(connection, time.monotonic() + self.request_timeout)
        if status != 0 or value != b"AUTH_RESULT":
            raise CacheResponseError(status, "authentication failed")
        connection.settimeout(None)

    def _take_retry_token(self) -> bool:
        with self._condition:
            now = time.monotonic()
            self._retry_tokens = min(10.0, self._retry_tokens + now - self._retry_updated_at)
            self._retry_updated_at = now
            if self._closed.is_set() or self._retry_tokens < 1:
                return False
            self._retry_tokens -= 1
            return True

    @staticmethod
    def _is_transient(error: OSError) -> bool:
        return isinstance(error, (TimeoutError, ConnectionError)) or error.errno in {
            errno.ECONNABORTED,
            errno.ECONNREFUSED,
            errno.ECONNRESET,
            errno.EHOSTUNREACH,
            errno.ENETDOWN,
            errno.ENETRESET,
            errno.ENETUNREACH,
            errno.EPIPE,
            errno.ETIMEDOUT,
        }

    def ping(self) -> bool:
        status, value = self._request(4)
        self._ensure_ok(status, value)
        if value != b"PONG":
            raise CacheProtocolError("PING response did not contain PONG")
        return True

    def set(self, key: str, value: CacheValue, ttl_secs: int = 0) -> None:
        encoded_key = self._encode_key(key)
        if isinstance(value, str):
            encoded_value = value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray, memoryview)):
            encoded_value = bytes(value)
        else:
            raise TypeError("value must be str or bytes-like")
        
        if len(encoded_value) > _MAX_VALUE_LEN:
            raise ValueError(f"value exceeds the {_MAX_VALUE_LEN}-byte protocol limit")
        
        if isinstance(ttl_secs, bool) or not isinstance(ttl_secs, int):
            raise ValueError("ttl_secs must be a non-negative 64-bit integer")
        
        if not 0 <= ttl_secs <= 0xFFFFFFFFFFFFFFFF:
            raise ValueError("ttl_secs must be a non-negative 64-bit integer")

        status, response_value = self._request(1, encoded_key, encoded_value, ttl_secs)
        self._ensure_ok(status, response_value)

    def get(self, key: str) -> bytes | None:
        encoded_key = self._encode_key(key)
        status, value = self._request(2, encoded_key)
        if status == 1:
            return None
        self._ensure_ok(status, value)
        return value

    def delete(self, key: str) -> bool:
        encoded_key = self._encode_key(key)
        status, value = self._request(3, encoded_key)
        if status == 1:
            return False
        self._ensure_ok(status, value)
        return True

    @staticmethod
    def _parse_address(address: str) -> tuple[str, int]:
        if not isinstance(address, str) or not address:
            raise ValueError("address must be a host:port string")
        
        if address.startswith("["):
            closing_bracket = address.find("]")
            if closing_bracket < 0 or address[closing_bracket + 1 : closing_bracket + 2] != ":":
                raise ValueError("IPv6 addresses must use the [host]:port format")
            host = address[1:closing_bracket]
            port_text = address[closing_bracket + 2 :]
        else:
            if address.count(":") != 1:
                raise ValueError("address must be a host:port string; bracket IPv6 addresses")
            host, port_text = address.rsplit(":", 1)

        if not host or not port_text.isdecimal():
            raise ValueError("address must be a host:port string")
        port = int(port_text)

        if not 1 <= port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        
        return host, port

    @staticmethod
    def _encode_key(key: str) -> bytes:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if not key:
            raise ValueError("key must not be empty")
        encoded_key = key.encode("utf-8")
        if len(encoded_key) > _MAX_KEY_LEN:
            raise ValueError(f"key exceeds the {_MAX_KEY_LEN}-byte protocol limit")
        return encoded_key

    def _request(
        self,
        opcode: int,
        key: bytes = b"",
        value: bytes = b"",
        ttl_secs: int = 0,
    ) -> tuple[int, bytes]:
        header = _REQUEST_HEADER.pack(
            _MAGIC,
            _VERSION,
            opcode,
            len(key),
            len(value),
            ttl_secs,
        )
        payload = header + key + value
        attempt = 0
        while True:
            connection = None
            healthy = False
            transport_error = None
            try:
                connection = self._acquire()
                deadline = time.monotonic() + self.request_timeout
                connection.socket.settimeout(self.request_timeout)
                connection.socket.sendall(payload)
                response = self._read_response(connection.socket, deadline)
                if opcode == 4 and response[0] == 0 and response[1] != b"PONG":
                    raise CacheProtocolError("PING response did not contain PONG")
                healthy = True
                return response
            except OSError as error:
                transport_error = error
            finally:
                if connection is not None:
                    self._release(connection, healthy)
            if (
                opcode != 2
                or attempt >= self.max_retries
                or not self._is_transient(transport_error)
                or not self._take_retry_token()
            ):
                raise CacheConnectionError(
                    f"Could not reach NebulaCache at {self.address}: {transport_error}"
                ) from transport_error
            delay = random.uniform(0.0, min(1.0, 0.05 * 2 ** min(attempt, 5)))
            attempt += 1
            if self._closed.wait(delay):
                raise CacheConnectionError("NebulaCache client is closed") from transport_error

    @staticmethod
    def _read_response(connection: socket.socket, deadline: float | None = None) -> tuple[int, bytes]:
        header = NebulaCache._receive_exactly(connection, _RESPONSE_HEADER.size, deadline)
        magic, version, status, value_len = _RESPONSE_HEADER.unpack(header)
        if magic != _MAGIC:
            raise CacheProtocolError("response has invalid magic bytes")
        if version != _VERSION:
            raise CacheProtocolError(f"unsupported protocol version: {version}")
        if status not in _STATUS_NAMES:
            raise CacheProtocolError(f"unknown response status: {status}")
        if value_len > _MAX_VALUE_LEN:
            raise CacheProtocolError(f"response exceeds the {_MAX_VALUE_LEN}-byte protocol limit")
        return status, NebulaCache._receive_exactly(connection, value_len, deadline)

    @staticmethod
    def _receive_exactly(
        connection: socket.socket, length: int, deadline: float | None = None
    ) -> bytes:
        chunks = bytearray()
        while len(chunks) < length:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("NebulaCache request timed out")
                connection.settimeout(remaining)
            chunk = connection.recv(length - len(chunks))
            if not chunk:
                raise ConnectionResetError("connection closed before the response was complete")
            chunks.extend(chunk)
        return bytes(chunks)

    @staticmethod
    def _ensure_ok(status: int, value: bytes) -> None:
        if status != 0:
            name = _STATUS_NAMES.get(status, f"UNKNOWN_{status}")
            message = value.decode("utf-8", errors="replace")
            raise CacheResponseError(status, f"{name}: {message}".rstrip())
