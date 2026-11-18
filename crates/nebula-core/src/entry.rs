use std::time::Instant;

/// Owned cache value with an optional monotonic expiration deadline.
///
/// Cloning preserves the original deadline rather than restarting the TTL. This type
/// does not own an eviction timer; the store decides when to reclaim expired values.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Opaque bytes; an empty value is distinct from an absent cache entry.
    pub value: Vec<u8>,
    /// `None` means no expiration; wall-clock changes do not affect this deadline.
    pub expires_at: Option<Instant>,
}

impl CacheEntry {
    /// Starts the TTL now; zero or an unrepresentable deadline means no expiration.
    /// The deadline belongs to this value, so replacing an entry starts a new TTL.
    pub fn new(value: Vec<u8>, ttl_secs: u64) -> Self {
        let expires_at = if ttl_secs == 0 {
            None
        } else {
            // Very large TTLs fall back to the same non-expiring representation as zero.
            Instant::now().checked_add(std::time::Duration::from_secs(ttl_secs))
        };
        Self { value, expires_at }
    }

    /// Tests whether the monotonic deadline has been reached, without removing the entry.
    /// Equality counts as expired; callers should not serve a value at its deadline.
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
}
