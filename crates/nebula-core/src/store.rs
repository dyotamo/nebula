use std::{
    io,
    sync::RwLock,
    time::{Instant, SystemTime},
};

use dashmap::DashMap;

use super::CacheEntry;

#[derive(Clone, Debug)]
pub struct PersistenceEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at: Option<SystemTime>,
}

#[derive(Default)]
pub struct CacheStore {
    entries: DashMap<Vec<u8>, CacheEntry>,
    snapshot_gate: RwLock<()>,
}

impl CacheStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl_secs: u64) {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.insert(key, CacheEntry::new(value, ttl_secs));
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let entry = self.entries.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return None;
        }
        Some(entry.value.clone())
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.remove(key).is_some()
    }

    pub fn clear(&self) {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.len()
    }

    pub fn memory_usage(&self) -> usize {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries
            .iter()
            .map(|entry| entry.key().len().saturating_add(entry.value().value.len()))
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.is_empty()
    }

    pub fn snapshot(&self) -> Vec<(Vec<u8>, Vec<u8>, Option<u64>)> {
        let _snapshot_guard = self
            .snapshot_gate
            .read()
            .unwrap_or_else(|error| error.into_inner());
        self.entries.retain(|_, entry| !entry.is_expired());
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
        let _snapshot_guard = self
            .snapshot_gate
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let instant_now = Instant::now();
        let system_now = SystemTime::now();
        let mut snapshot = Vec::with_capacity(self.entries.len());

        self.entries.retain(|_, entry| !entry.is_expired());
        snapshot.extend(self.entries.iter().map(|entry| PersistenceEntry {
            key: entry.key().clone(),
            value: entry.value().value.clone(),
            expires_at: entry.expires_at.and_then(|deadline| {
                system_now.checked_add(deadline.saturating_duration_since(instant_now))
            }),
        }));
        snapshot
    }

    pub fn restore_from_persistence(&self, entries: Vec<PersistenceEntry>) -> io::Result<usize> {
        let _snapshot_guard = self
            .snapshot_gate
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let system_now = SystemTime::now();
        let instant_now = Instant::now();
        let mut restored = Vec::with_capacity(entries.len());

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
            restored.push((
                entry.key,
                CacheEntry {
                    value: entry.value,
                    expires_at,
                },
            ));
        }

        self.entries.clear();
        for (key, entry) in restored {
            self.entries.insert(key, entry);
        }

        Ok(self.entries.len())
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
    }
}
