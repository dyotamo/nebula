mod entry;
mod store;

use serde::Deserialize;

pub use entry::CacheEntry;
pub use store::{CacheEntryMetadata, CacheStore, PersistenceEntry};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    #[default]
    Lru,
    Lfu,
    Fifo,
}
