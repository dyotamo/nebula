use std::{
    collections::HashMap,
    io,
    sync::{Mutex, RwLock},
    time::{Instant, SystemTime},
};

use dashmap::DashMap;

use super::{CacheEntry, EvictionPolicy};

#[derive(Clone, Debug)]
pub struct PersistenceEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at: Option<SystemTime>,
}

#[derive(Default)]
struct EntryMetadata {
    size: usize,
    inserted: u64,
    accessed: u64,
    hits: u64,
}

#[derive(Default)]
struct CacheAccounting {
    total_bytes: usize,
    clock: u64,
    entries: HashMap<Vec<u8>, EntryMetadata>,
}

pub struct CacheStore {
    entries: DashMap<Vec<u8>, CacheEntry>,
    snapshot_gate: RwLock<()>,
    accounting: Mutex<CacheAccounting>,
    max_size_bytes: usize,
    eviction_policy: EvictionPolicy,
}

impl Default for CacheStore {
    fn default() -> Self {
        Self::with_limits(usize::MAX, EvictionPolicy::Lru)
    }
}

impl CacheStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_limits(max_size_bytes: usize, eviction_policy: EvictionPolicy) -> Self {
        Self {
            entries: DashMap::new(),
            snapshot_gate: RwLock::new(()),
            accounting: Mutex::new(CacheAccounting::default()),
            max_size_bytes,
            eviction_policy,
        }
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl_secs: u64) -> bool {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let size = key.len().saturating_add(value.len());
        if size > self.max_size_bytes {
            return false;
        }

        Self::remove_key(&self.entries, &mut accounting, &key);
        if accounting.total_bytes.saturating_add(size) > self.max_size_bytes {
            Self::remove_expired(&self.entries, &mut accounting);
        }
        while accounting.total_bytes.saturating_add(size) > self.max_size_bytes {
            let Some(victim) = Self::victim_key(&accounting, self.eviction_policy) else {
                return false;
            };
            Self::remove_key(&self.entries, &mut accounting, &victim);
        }

        let tick = Self::next_tick(&mut accounting);
        accounting.total_bytes = accounting.total_bytes.saturating_add(size);
        accounting.entries.insert(
            key.clone(),
            EntryMetadata {
                size,
                inserted: tick,
                accessed: tick,
                hits: 0,
            },
        );
        self.entries.insert(key, CacheEntry::new(value, ttl_secs));
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let entry = self.entries.get(key)?;
        if entry.is_expired() {
            drop(entry);
            Self::remove_key(&self.entries, &mut accounting, key);
            return None;
        }
        let value = entry.value.clone();
        drop(entry);

        let tick = Self::next_tick(&mut accounting);
        if let Some(metadata) = accounting.entries.get_mut(key) {
            metadata.accessed = tick;
            metadata.hits = metadata.hits.saturating_add(1);
        }
        Some(value)
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        Self::remove_key(&self.entries, &mut accounting, key)
    }

    pub fn clear(&self) {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.clear();
        accounting.total_bytes = 0;
        accounting.entries.clear();
    }

    pub fn len(&self) -> usize {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.len()
    }

    pub fn memory_usage(&self) -> usize {
        self.accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .total_bytes
    }

    pub fn is_empty(&self) -> bool {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.is_empty()
    }

    pub fn snapshot(&self) -> Vec<(Vec<u8>, Vec<u8>, Option<u64>)> {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        Self::remove_expired(&self.entries, &mut accounting);
        self.entries
            .iter()
            .map(|entry| {
                let ttl_secs = entry.expires_at.map(|deadline| {
                    deadline
                        .saturating_duration_since(std::time::Instant::now())
                        .as_secs()
                });
                (entry.key().clone(), entry.value().value.clone(), ttl_secs)
            })
            .collect()
    }

    pub fn snapshot_for_persistence(&self) -> Vec<PersistenceEntry> {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let instant_now = Instant::now();
        let system_now = SystemTime::now();
        Self::remove_expired(&self.entries, &mut accounting);
        self.entries
            .iter()
            .map(|entry| PersistenceEntry {
                key: entry.key().clone(),
                value: entry.value().value.clone(),
                expires_at: entry.expires_at.and_then(|deadline| {
                    system_now.checked_add(deadline.saturating_duration_since(instant_now))
                }),
            })
            .collect()
    }

    pub fn restore_from_persistence(&self, entries: Vec<PersistenceEntry>) -> io::Result<usize> {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let system_now = SystemTime::now();
        let instant_now = Instant::now();
        self.entries.clear();
        accounting.total_bytes = 0;
        accounting.entries.clear();

        for entry in entries {
            let expires_at = match entry.expires_at {
                Some(deadline) => {
                    let Ok(remaining) = deadline.duration_since(system_now) else {
                        continue;
                    };
                    Some(instant_now.checked_add(remaining).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "cache expiration is out of range",
                        )
                    })?)
                }
                None => None,
            };
            let size = entry.key.len().saturating_add(entry.value.len());
            if size > self.max_size_bytes {
                continue;
            }
            Self::remove_key(&self.entries, &mut accounting, &entry.key);
            let tick = Self::next_tick(&mut accounting);
            accounting.total_bytes = accounting.total_bytes.saturating_add(size);
            accounting.entries.insert(
                entry.key.clone(),
                EntryMetadata {
                    size,
                    inserted: tick,
                    accessed: tick,
                    hits: 0,
                },
            );
            self.entries.insert(
                entry.key,
                CacheEntry {
                    value: entry.value,
                    expires_at,
                },
            );
        }

        while accounting.total_bytes > self.max_size_bytes {
            let Some(victim) = Self::victim_key(&accounting, self.eviction_policy) else {
                break;
            };
            Self::remove_key(&self.entries, &mut accounting, &victim);
        }

        Ok(self.entries.len())
    }

    fn next_tick(accounting: &mut CacheAccounting) -> u64 {
        accounting.clock = accounting.clock.saturating_add(1);
        accounting.clock
    }

    fn remove_key(
        entries: &DashMap<Vec<u8>, CacheEntry>,
        accounting: &mut CacheAccounting,
        key: &[u8],
    ) -> bool {
        let removed = entries.remove(key).is_some();
        if let Some(metadata) = accounting.entries.remove(key) {
            accounting.total_bytes = accounting.total_bytes.saturating_sub(metadata.size);
        }
        removed
    }

    fn remove_expired(entries: &DashMap<Vec<u8>, CacheEntry>, accounting: &mut CacheAccounting) {
        let expired: Vec<_> = accounting
            .entries
            .keys()
            .filter(|key| {
                entries
                    .get(key.as_slice())
                    .is_none_or(|entry| entry.is_expired())
            })
            .cloned()
            .collect();
        for key in expired {
            Self::remove_key(entries, accounting, &key);
        }
    }

    fn victim_key(accounting: &CacheAccounting, policy: EvictionPolicy) -> Option<Vec<u8>> {
        accounting
            .entries
            .iter()
            .min_by_key(|(_, metadata)| match policy {
                EvictionPolicy::Lru => (metadata.accessed, metadata.inserted),
                EvictionPolicy::Lfu => (metadata.hits, metadata.accessed),
                EvictionPolicy::Fifo => (metadata.inserted, 0),
            })
            .map(|(key, _)| key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn stores_and_reads_value() {
        let store = CacheStore::new();
        store.set(b"k".to_vec(), b"value".to_vec(), 0);
        assert_eq!(store.get(b"k"), Some(b"value".to_vec()));
    }

    #[test]
    fn expires_values() {
        let store = CacheStore::new();
        store.set(b"short".to_vec(), b"value".to_vec(), 1);
        thread::sleep(Duration::from_millis(1_100));
        assert_eq!(store.get(b"short"), None);
    }

    #[test]
    fn deletes_values() {
        let store = CacheStore::new();
        store.set(b"k".to_vec(), b"v".to_vec(), 0);
        assert!(store.delete(b"k"));
        assert!(!store.delete(b"k"));
        assert_eq!(store.memory_usage(), 0);
    }

    #[test]
    fn lru_evicts_the_least_recently_used_entry() {
        let store = CacheStore::with_limits(4, EvictionPolicy::Lru);
        store.set(b"a".to_vec(), b"1".to_vec(), 0);
        store.set(b"b".to_vec(), b"2".to_vec(), 0);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        store.set(b"c".to_vec(), b"3".to_vec(), 0);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b"), None);
        assert_eq!(store.memory_usage(), 4);
    }

    #[test]
    fn lfu_evicts_the_least_frequently_used_entry() {
        let store = CacheStore::with_limits(4, EvictionPolicy::Lfu);
        store.set(b"a".to_vec(), b"1".to_vec(), 0);
        store.set(b"b".to_vec(), b"2".to_vec(), 0);
        store.get(b"a");
        store.get(b"a");
        store.set(b"c".to_vec(), b"3".to_vec(), 0);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b"), None);
    }

    #[test]
    fn fifo_evicts_the_oldest_entry_regardless_of_access() {
        let store = CacheStore::with_limits(4, EvictionPolicy::Fifo);
        store.set(b"a".to_vec(), b"1".to_vec(), 0);
        store.set(b"b".to_vec(), b"2".to_vec(), 0);
        store.get(b"a");
        store.set(b"c".to_vec(), b"3".to_vec(), 0);
        assert_eq!(store.get(b"a"), None);
        assert_eq!(store.get(b"b"), Some(b"2".to_vec()));
    }

    #[test]
    fn rejects_an_entry_larger_than_the_configured_limit() {
        let store = CacheStore::with_limits(2, EvictionPolicy::Lru);
        assert!(!store.set(b"k".to_vec(), b"value".to_vec(), 0));
        assert!(store.is_empty());
        assert_eq!(store.memory_usage(), 0);
    }
}
