//! In-memory byte cache with TTL expiration, capacity accounting, and eviction.
//!
//! Keys and values remain opaque bytes; protocol validation and authorization belong
//! to callers. Live TTL decisions use monotonic time, while persistence records use
//! wall-clock deadlines so process downtime still consumes the remaining lifetime.
//! Administrative snapshots contain metadata only and may lag current contents.

mod entry;
mod store;

use serde::Deserialize;

pub use entry::CacheEntry;
pub use store::{CacheEntryMetadata, CacheStore, PersistenceEntry};

/// Chooses which entry to evict when the key-plus-value capacity is exceeded.
///
/// Capacity pressure first reclaims expired entries. Policy history is process-local
/// and resets for a key whenever its value is replaced or restored.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    /// Least recently read or written.
    /// Successful value reads refresh recency; metadata inspection does not.
    #[default]
    Lru,
    /// Fewest reads since insertion or replacement, breaking ties by recency.
    /// New values start with zero hits, so one read can distinguish otherwise fresh keys.
    Lfu,
    /// Oldest insertion or replacement, regardless of reads.
    /// Replacing a value moves its key to the newest end of this ordering.
    Fifo,
}
