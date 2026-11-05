# Nebula binary protocol v1

All integers are big-endian. TCP is a byte stream; implementations MUST read
the full fixed header and then the declared payload lengths.

## Request
| Field | Bytes |
|---|---:|
| Magic (`NEBU`) | 4 |
| Version (`1`) | 1 |
| Opcode | 1 |
| Key length | 4 |
| Value length | 4 |
| TTL seconds | 8 |
| Key | key length |
| Value | value length |

Opcodes: 1 SET, 2 GET, 3 DELETE, 4 PING.
GET and DELETE require a non-empty key and zero value length.
PING has no key/value. SET requires a non-empty key.
Maximum key: 1024 bytes. Maximum value: 8 MiB.

## Response
| Field | Bytes |
|---|---:|
| Magic (`NEBU`) | 4 |
| Version (`1`) | 1 |
| Status | 1 |
| Value length | 4 |
| Value | value length |

Statuses: 0 OK, 1 NOT_FOUND, 2 INVALID_ARGUMENT, 3 INTERNAL_ERROR.
