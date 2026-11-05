# NebulaCache architecture — v0.1

## Phase 1: single-node storage engine

This phase implements a real TCP cache node. Each process owns an independent
in-memory key/value store. The three Compose services are isolated instances
for development and protocol testing; they do NOT yet form a distributed cluster.

### Components
- `nebula-protocol`: versioned binary wire protocol.
- `nebula-node`: Tokio TCP server, request handler and in-memory engine.
- `CacheStore`: concurrent map, lazy TTL expiration and basic CRUD.

### Protocol
Request header (22 bytes):
- magic: 4 bytes (`NEBU`)
- version: u8
- opcode: u8
- key length: u32 big-endian
- value length: u32 big-endian
- TTL seconds: u64 big-endian
- key bytes, then value bytes

Response header (10 bytes):
- magic: 4 bytes
- version: u8
- status: u8
- value length: u32 big-endian
- value bytes

Limits: key <= 1 KiB; value <= 8 MiB.

### Important semantics
- TTL 0 means no expiration.
- Expiration is lazy: an expired item is removed when read.
- Values are opaque bytes; serialization belongs to clients.
- No persistence: process restart loses all entries.
- No authentication or TLS in this development phase.
- No replication, routing, quorum, membership or failover yet.

## Distributed evolution
Phase 2 adds membership and consistent hashing.
Phase 3 adds RF=3 replication and quorum operations.
For N=3, W=2 and R=2 satisfy R+W>N, but quorum intersection alone does
not provide linearizability. A later design must define leader/primary
ownership, epochs/terms, conflict handling, read/write ordering and behavior
during network partitions.
