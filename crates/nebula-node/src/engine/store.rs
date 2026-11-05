use dashmap::DashMap;

use super::CacheEntry;

#[derive(Default)]
pub struct CacheStore {
    entries: DashMap<Vec<u8>, CacheEntry>,
}

impl CacheStore {
    pub fn new() -> Self { Self::default() }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl_secs: u64) {
        self.entries.insert(key, CacheEntry::new(value, ttl_secs));
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.entries.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return None;
        }
        Some(entry.value.clone())
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.entries.remove(key).is_some()
    }

    pub fn len(&self) -> usize { self.entries.len() }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
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
