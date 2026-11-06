use std::time::Instant;

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: Vec<u8>,
    pub expires_at: Option<Instant>,
}

impl CacheEntry {
    pub fn new(value: Vec<u8>, ttl_secs: u64) -> Self {
        let expires_at = if ttl_secs == 0 {
            None
        } else {
            Instant::now().checked_add(std::time::Duration::from_secs(ttl_secs))
        };
        Self { value, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
}
