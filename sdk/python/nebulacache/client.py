from __future__ import annotations

import socket
import struct
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
}


class NebulaCache:
    """Synchronous client for the NebulaCache binary TCP protocol."""

    def __init__(self, address: str = "127.0.0.1:7001", timeout: float = 5.0) -> None:
        self.host, self.port = self._parse_address(address)
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self.address = address
        self.timeout = timeout

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
        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout) as connection:
                connection.settimeout(self.timeout)
                connection.sendall(header + key + value)
                return self._read_response(connection)
        except OSError as error:
            raise CacheConnectionError(f"Could not reach NebulaCache at {self.address}: {error}") from error

    @staticmethod
    def _read_response(connection: socket.socket) -> tuple[int, bytes]:
        header = NebulaCache._receive_exactly(connection, _RESPONSE_HEADER.size)
        magic, version, status, value_len = _RESPONSE_HEADER.unpack(header)
        if magic != _MAGIC:
            raise CacheProtocolError("response has invalid magic bytes")
        if version != _VERSION:
            raise CacheProtocolError(f"unsupported protocol version: {version}")
        if status not in _STATUS_NAMES:
            raise CacheProtocolError(f"unknown response status: {status}")
        if value_len > _MAX_VALUE_LEN:
            raise CacheProtocolError(f"response exceeds the {_MAX_VALUE_LEN}-byte protocol limit")
        return status, NebulaCache._receive_exactly(connection, value_len)

    @staticmethod
    def _receive_exactly(connection: socket.socket, length: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < length:
            chunk = connection.recv(length - len(chunks))
            if not chunk:
                raise CacheProtocolError("connection closed before the response was complete")
            chunks.extend(chunk)
        return bytes(chunks)

    @staticmethod
    def _ensure_ok(status: int, value: bytes) -> None:
        if status != 0:
            name = _STATUS_NAMES.get(status, f"UNKNOWN_{status}")
            message = value.decode("utf-8", errors="replace")
            raise CacheResponseError(status, f"{name}: {message}".rstrip())
