use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant, SystemTime},
};

use dashmap::DashMap;

use super::{CacheEntry, EvictionPolicy};

#[derive(Clone, Debug)]
pub struct PersistenceEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at: Option<SystemTime>,
}

#[derive(Clone, Debug)]
pub struct CacheEntryMetadata {
    pub key: Vec<u8>,
    pub size: usize,
    pub created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
    pub last_accessed: SystemTime,
    expires_at_instant: Option<Instant>,
}

impl CacheEntryMetadata {
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(Instant::now())
    }

    pub fn is_expired_at(&self, now: Instant) -> bool {
        self.expires_at_instant
            .is_some_and(|expires_at| now >= expires_at)
    }

    pub fn remaining_ttl_secs_at(&self, now: Instant) -> Option<u64> {
        self.expires_at_instant.map(|expires_at| {
            let remaining = expires_at.saturating_duration_since(now);
            remaining
                .as_secs()
                .saturating_add(u64::from(remaining.subsec_nanos() > 0))
        })
    }
}

struct EntryMetadata {
    size: usize,
    inserted: u64,
    accessed: u64,
    hits: u64,
    created_at: SystemTime,
    expires_at: Option<SystemTime>,
    expires_at_instant: Option<Instant>,
    last_accessed: SystemTime,
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
    admin_snapshot: RwLock<Option<(Instant, Arc<Vec<CacheEntryMetadata>>)>>,
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
            admin_snapshot: RwLock::new(None),
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
        let entry = CacheEntry::new(value, ttl_secs);
        let created_at = SystemTime::now();
        let expires_at = entry.expires_at.and_then(|deadline| {
            SystemTime::now().checked_add(deadline.saturating_duration_since(Instant::now()))
        });
        accounting.total_bytes = accounting.total_bytes.saturating_add(size);
        accounting.entries.insert(
            key.clone(),
            EntryMetadata {
                size,
                inserted: tick,
                accessed: tick,
                hits: 0,
                created_at,
                expires_at,
                expires_at_instant: entry.expires_at,
                last_accessed: created_at,
            },
        );
        self.entries.insert(key, entry);
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_inner(key, |value, _| value)
    }

    /// Reads the value from the primary store and returns metadata from the same
    /// synchronized lookup.
    pub fn get_with_metadata(&self, key: &[u8]) -> Option<(Vec<u8>, CacheEntryMetadata)> {
        self.get_inner(key, |value, metadata| {
            metadata.map(|metadata| (value, Self::admin_metadata(key, metadata)))
        })
        .flatten()
    }

    fn get_inner<R>(
        &self,
        key: &[u8],
        result: impl FnOnce(Vec<u8>, Option<&EntryMetadata>) -> R,
    ) -> Option<R> {
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
        let mut metadata = accounting.entries.get_mut(key);
        if let Some(metadata) = metadata.as_deref_mut() {
            metadata.accessed = tick;
            metadata.hits = metadata.hits.saturating_add(1);
            metadata.last_accessed = SystemTime::now();
        }
        Some(result(value, metadata.as_deref()))
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

    /// Returns an immutable, key-ordered metadata projection, refreshed on demand
    /// at least once per second. A returned snapshot remains stable for its caller.
    pub fn admin_snapshot(&self) -> Arc<Vec<CacheEntryMetadata>> {
        if let Some((built_at, snapshot)) = self
            .admin_snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            if built_at.elapsed() < Duration::from_secs(1) {
                return Arc::clone(snapshot);
            }
        }

        let mut cached = self
            .admin_snapshot
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if let Some((built_at, snapshot)) = cached.as_ref() {
            if built_at.elapsed() < Duration::from_secs(1) {
                return Arc::clone(snapshot);
            }
        }
        let (built_at, mut entries): (Instant, Vec<CacheEntryMetadata>) = {
            let accounting = self
                .accounting
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let entries = accounting
                .entries
                .iter()
                .map(|(key, metadata)| Self::admin_metadata(key, metadata))
                .collect();
            (Instant::now(), entries)
        };
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        let snapshot = Arc::new(entries);
        *cached = Some((built_at, Arc::clone(&snapshot)));
        snapshot
    }

    /// Looks up metadata directly in the registry and removes an expired entry.
    pub fn admin_entry(&self, key: &[u8]) -> Option<CacheEntryMetadata> {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let now = Instant::now();
        let expired = accounting
            .entries
            .get(key)
            .is_some_and(|metadata| metadata.expires_at_instant.is_some_and(|time| now >= time));
        if expired {
            Self::remove_key(&self.entries, &mut accounting, key);
            return None;
        }
        accounting
            .entries
            .get(key)
            .map(|metadata| Self::admin_metadata(key, metadata))
    }

    pub fn is_empty(&self) -> bool {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.is_empty()
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut accounting = self
            .accounting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        Self::remove_expired(&self.entries, &mut accounting)
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
                    created_at: SystemTime::now(),
                    expires_at: entry.expires_at,
                    expires_at_instant: expires_at,
                    last_accessed: SystemTime::now(),
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

    fn admin_metadata(key: &[u8], metadata: &EntryMetadata) -> CacheEntryMetadata {
        CacheEntryMetadata {
            key: key.to_vec(),
            size: metadata.size,
            created_at: metadata.created_at,
            expires_at: metadata.expires_at,
            last_accessed: metadata.last_accessed,
            expires_at_instant: metadata.expires_at_instant,
        }
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

    fn remove_expired(
        entries: &DashMap<Vec<u8>, CacheEntry>,
        accounting: &mut CacheAccounting,
    ) -> usize {
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
        let removed = expired.len();
        for key in expired {
            Self::remove_key(entries, accounting, &key);
        }
        removed
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
    fn cleanup_removes_expired_values_and_updates_memory_usage() {
        let store = CacheStore::new();
        store.set(b"short".to_vec(), b"value".to_vec(), 1);
        store.set(b"persistent".to_vec(), b"value".to_vec(), 0);
        thread::sleep(Duration::from_millis(1_100));

        assert_eq!(store.cleanup_expired(), 1);
        assert_eq!(store.get(b"short"), None);
        assert_eq!(store.get(b"persistent"), Some(b"value".to_vec()));
        assert_eq!(store.memory_usage(), b"persistent".len() + b"value".len());
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
        assert_eq!(
            store
                .admin_snapshot()
                .iter()
                .map(|entry| entry.key.as_slice())
                .collect::<Vec<_>>(),
            [b"a".as_slice(), b"c".as_slice()]
        );
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

    #[test]
    fn admin_snapshot_is_sorted_metadata_and_tracks_mutations() {
        let store = CacheStore::new();
        store.set(b"b".to_vec(), b"secret".to_vec(), 0);
        store.set(b"a".to_vec(), b"value".to_vec(), 30);

        let first = store.admin_snapshot();
        assert_eq!(
            first
                .iter()
                .map(|entry| entry.key.as_slice())
                .collect::<Vec<_>>(),
            [b"a".as_slice(), b"b".as_slice()]
        );
        assert_eq!(first[0].size, b"a".len() + b"value".len());
        assert!(first[0].expires_at.is_some());
        assert!(first[1].expires_at.is_none());

        store.set(b"b".to_vec(), b"updated".to_vec(), 0);
        store.delete(b"a");
        thread::sleep(Duration::from_millis(1_100));
        let second = store.admin_snapshot();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].key, b"b");
        assert_eq!(second[0].size, b"b".len() + b"updated".len());

        store.clear();
        thread::sleep(Duration::from_millis(1_100));
        assert!(store.admin_snapshot().is_empty());
        assert_eq!(first.len(), 2);
    }

    #[test]
    fn admin_inspection_removes_expired_entries() {
        let store = CacheStore::new();
        store.set(b"short".to_vec(), b"value".to_vec(), 1);
        thread::sleep(Duration::from_millis(1_100));

        assert!(store.admin_entry(b"short").is_none());
        assert!(store.admin_snapshot().is_empty());
        assert_eq!(store.memory_usage(), 0);
    }

    #[test]
    fn restore_rebuilds_entry_metadata() {
        let store = CacheStore::new();
        store.set(b"old".to_vec(), b"value".to_vec(), 0);
        let restored = store
            .restore_from_persistence(vec![PersistenceEntry {
                key: b"restored".to_vec(),
                value: b"value".to_vec(),
                expires_at: None,
            }])
            .unwrap();

        assert_eq!(restored, 1);
        let snapshot = store.admin_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].key, b"restored");
        assert_eq!(snapshot[0].size, b"restored".len() + b"value".len());
        assert!(snapshot[0].expires_at.is_none());
    }

    #[test]
    fn admin_snapshots_remain_safe_during_concurrent_mutations() {
        let store = Arc::new(CacheStore::new());
        let writers: Vec<_> = (0..4)
            .map(|writer| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    for index in 0..100 {
                        let key = format!("{writer}:{index}").into_bytes();
                        store.set(key, b"value".to_vec(), 0);
                        let snapshot = store.admin_snapshot();
                        assert!(snapshot.windows(2).all(|pair| pair[0].key <= pair[1].key));
                    }
                })
            })
            .collect();

        for writer in writers {
            writer.join().unwrap();
        }
        thread::sleep(Duration::from_millis(1_100));
        assert_eq!(store.admin_snapshot().len(), 400);
    }
}
