use std::{
    fs, io,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use futures::TryStreamExt;
use nebula_core::{CacheStore, PersistenceEntry};
use nebula_health::{lifecycle::ApplicationState, ComponentHealth, HealthComponent, HealthManager};
use object_store::{
    aws::AmazonS3Builder, local::LocalFileSystem, path::Path as ObjectPath, ObjectStore,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Mutex,
    time::{interval_at, Instant, MissedTickBehavior},
};
use tracing::{error, info, warn};

use crate::{
    config::{PersistenceOptions, SnapshotBackend},
    metrics::Metrics,
};

const MAGIC: &[u8; 4] = b"NBSC";
const FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = 12;
const SNAPSHOT_PREFIX: &str = "snapshot-";
const SNAPSHOT_EXTENSION: &str = ".nbs";
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub(crate) struct PersistenceManager {
    storage: Result<Arc<dyn ObjectStore>, String>,
    prefix: String,
    storage_label: String,
    interval: Duration,
    retention: usize,
    operation: Arc<Mutex<()>>,
    health: HealthManager,
    metrics: Arc<Metrics>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotPayload {
    created_at: Timestamp,
    entries: Vec<SnapshotEntry>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    expires_at: Option<Timestamp>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
struct Timestamp {
    seconds: u64,
    nanos: u32,
}

struct PersistenceRecorder {
    metrics: Arc<Metrics>,
    started: std::time::Instant,
    snapshot: bool,
    success: bool,
}

impl PersistenceRecorder {
    fn new(metrics: Arc<Metrics>, snapshot: bool) -> Self {
        Self {
            metrics,
            started: std::time::Instant::now(),
            snapshot,
            success: false,
        }
    }
}

impl Drop for PersistenceRecorder {
    fn drop(&mut self) {
        let elapsed_ns = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        if self.snapshot {
            self.metrics.record_snapshot(elapsed_ns, self.success);
        } else {
            self.metrics.record_recovery(elapsed_ns, self.success);
        }
    }
}

impl PersistenceManager {
    pub(crate) fn new(
        options: PersistenceOptions,
        interval: Duration,
        retention: usize,
        health: HealthManager,
        metrics: Arc<Metrics>,
    ) -> Self {
        let prefix = if options.snapshot_backend == SnapshotBackend::S3 {
            options.snapshot_s3_prefix.trim_matches('/').to_owned()
        } else {
            String::new()
        };
        let (storage, storage_label) = match options.snapshot_backend {
            SnapshotBackend::Filesystem => {
                let label = options.snapshot_dir.display().to_string();
                let storage = fs::create_dir_all(&options.snapshot_dir)
                    .and_then(|_| {
                        LocalFileSystem::new_with_prefix(&options.snapshot_dir)
                            .map_err(io::Error::other)
                    })
                    .map(|storage| Arc::new(storage) as Arc<dyn ObjectStore>)
                    .map_err(|error| error.to_string());
                (storage, label)
            }
            SnapshotBackend::S3 => {
                let bucket = options.snapshot_s3_bucket.clone();
                let storage = bucket
                    .ok_or_else(|| {
                        "persistence.snapshot_s3_bucket must be set for S3 storage".to_owned()
                    })
                    .and_then(|bucket| {
                        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                        if let Some(region) = options.snapshot_s3_region.clone() {
                            builder = builder.with_region(region);
                        }
                        if let Some(endpoint) = options.snapshot_s3_endpoint.as_deref() {
                            builder = builder.with_endpoint(endpoint);
                            if endpoint.starts_with("http://") {
                                builder = builder.with_allow_http(true);
                            }
                        }
                        if let Some(access_key_id) = options.snapshot_s3_access_key_id.as_deref() {
                            builder = builder.with_access_key_id(access_key_id);
                        }
                        if let Some(secret_access_key) =
                            options.snapshot_s3_secret_access_key.as_deref()
                        {
                            builder = builder.with_secret_access_key(secret_access_key);
                        }
                        builder
                            .build()
                            .map(|storage| Arc::new(storage) as Arc<dyn ObjectStore>)
                            .map_err(|error| error.to_string())
                    });
                let label = format!(
                    "s3://{}{}",
                    options.snapshot_s3_bucket.as_deref().unwrap_or(""),
                    if prefix.is_empty() {
                        String::new()
                    } else {
                        format!("/{prefix}")
                    }
                );
                (storage, label)
            }
        };
        Self {
            storage,
            prefix,
            storage_label,
            interval,
            retention,
            operation: Arc::new(Mutex::new(())),
            health,
            metrics,
        }
    }

    pub(crate) async fn restore(&self, store: Arc<CacheStore>) -> io::Result<bool> {
        let mut recorder = PersistenceRecorder::new(Arc::clone(&self.metrics), false);
        info!(storage = %self.storage_label, "cache snapshot restoration started");
        let object_store = match &self.storage {
            Ok(storage) => Arc::clone(storage),
            Err(error) => {
                let error = io::Error::other(error.clone());
                self.report_failure(&error);
                return Err(error);
            }
        };
        cleanup_interrupted_store_snapshots(object_store.as_ref(), &self.prefix).await;
        let loaded = match load_latest_from_store(object_store, &self.prefix).await {
            Ok(loaded) => loaded,
            Err(error) => {
                self.report_failure(&error);
                return Err(error);
            }
        };

        let Some((path, payload)) = loaded else {
            info!(storage = %self.storage_label, "no snapshots found; starting with an empty cache");
            self.report_success();
            recorder.success = true;
            return Ok(false);
        };

        let entries: Vec<_> = payload
            .entries
            .into_iter()
            .map(|entry| {
                Ok(PersistenceEntry {
                    key: entry.key,
                    value: entry.value,
                    expires_at: entry
                        .expires_at
                        .map(Timestamp::to_system_time)
                        .transpose()?,
                })
            })
            .collect::<io::Result<_>>()?;
        let restored = match store.restore_from_persistence(entries) {
            Ok(restored) => restored,
            Err(error) => {
                self.report_failure(&error);
                return Err(error);
            }
        };
        info!(path = %path, entries = restored, "cache snapshot restored");
        self.report_success();
        recorder.success = true;
        Ok(true)
    }

    pub(crate) async fn snapshot(&self, store: Arc<CacheStore>) -> io::Result<()> {
        let mut recorder = PersistenceRecorder::new(Arc::clone(&self.metrics), true);
        let _operation = Arc::clone(&self.operation).lock_owned().await;
        let object_store = match &self.storage {
            Ok(storage) => Arc::clone(storage),
            Err(error) => {
                let error = io::Error::other(error.clone());
                self.report_failure(&error);
                return Err(error);
            }
        };
        let captured = store.snapshot_for_persistence();
        let payload = payload_from_entries(captured)?;
        let object_name = next_snapshot_name(payload.created_at);
        let path = snapshot_object_path(&self.prefix, &object_name)?;

        info!(
            entries = payload.entries.len(),
            "cache snapshot serialization started"
        );
        let result = async {
            object_store
                .put(&path, encode_snapshot(&payload)?.into())
                .await
                .map_err(|error| io::Error::other(format!("write snapshot: {error}")))?;
            retain_store_snapshots(&object_store, &self.prefix, self.retention).await?;
            Ok::<_, io::Error>(())
        }
        .await;
        match result {
            Ok(()) => {
                info!(path = %path, "cache snapshot completed and synchronized");
                self.report_success();
                recorder.success = true;
                Ok(())
            }
            Err(error) => {
                self.report_failure(&error);
                Err(error)
            }
        }
    }

    pub(crate) async fn run(
        self,
        store: Arc<CacheStore>,
        mut lifecycle: tokio::sync::watch::Receiver<ApplicationState>,
    ) {
        let mut snapshots = interval_at(Instant::now() + self.interval, self.interval);
        snapshots.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = snapshots.tick() => {
                    if let Err(error) = self.snapshot(Arc::clone(&store)).await {
                        error!(error = %error, "periodic cache snapshot failed");
                    }
                }
                changed = lifecycle.changed() => {
                    if changed.is_err() || matches!(
                        *lifecycle.borrow(),
                        ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
                    ) {
                        return;
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn lock_snapshot_operation(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.operation).lock_owned().await
    }

    fn report_failure(&self, error: &io::Error) {
        self.health.report(
            HealthComponent::Persistence("snapshots".to_owned()),
            ComponentHealth::Unhealthy(error.to_string()),
        );
    }

    fn report_success(&self) {
        self.health.report(
            HealthComponent::Persistence("snapshots".to_owned()),
            ComponentHealth::Healthy,
        );
    }
}

fn snapshot_object_path(prefix: &str, name: &str) -> io::Result<ObjectPath> {
    let key = if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}/{name}")
    };
    ObjectPath::parse(key).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}

fn next_snapshot_name(created_at: Timestamp) -> String {
    let sequence = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "{SNAPSHOT_PREFIX}{}-{}-{}-{sequence}{SNAPSHOT_EXTENSION}",
        created_at.seconds,
        created_at.nanos,
        std::process::id()
    )
}

async fn snapshot_paths(store: &dyn ObjectStore, prefix: &str) -> io::Result<Vec<ObjectPath>> {
    let objects = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| io::Error::other(format!("list snapshots: {error}")))?;
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    Ok(objects
        .into_iter()
        .filter_map(|object| {
            let key = object.location.to_string();
            let relative = if prefix.is_empty() {
                key.as_str()
            } else {
                key.strip_prefix(&prefix)?
            };
            (!relative.contains('/')
                && relative.starts_with(SNAPSHOT_PREFIX)
                && relative.ends_with(SNAPSHOT_EXTENSION))
            .then_some(object.location)
        })
        .collect())
}

async fn cleanup_interrupted_store_snapshots(store: &dyn ObjectStore, prefix: &str) {
    let objects = match store.list(None).try_collect::<Vec<_>>().await {
        Ok(objects) => objects,
        Err(error) => {
            warn!(error = %error, "could not inspect interrupted snapshot files");
            return;
        }
    };
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    for object in objects {
        let key = object.location.to_string();
        let Some(name) = key.strip_prefix(&prefix) else {
            continue;
        };
        if !name.contains('/') && name.starts_with(".snapshot-") && name.ends_with(".tmp") {
            if let Err(error) = store.delete(&object.location).await {
                warn!(path = %object.location, error = %error, "could not remove interrupted snapshot temporary file");
            }
        }
    }
}

async fn read_store_snapshot(
    store: &dyn ObjectStore,
    path: &ObjectPath,
) -> io::Result<SnapshotPayload> {
    let result = store
        .get(path)
        .await
        .map_err(|error| io::Error::other(format!("read snapshot {path}: {error}")))?;
    let bytes = result
        .bytes()
        .await
        .map_err(|error| io::Error::other(format!("read snapshot {path}: {error}")))?;
    decode_snapshot(&bytes)
}

async fn load_latest_from_store(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> io::Result<Option<(ObjectPath, SnapshotPayload)>> {
    let candidates = snapshot_paths(store.as_ref(), prefix).await?;
    let mut snapshots = Vec::new();
    let mut failures = Vec::new();
    for path in &candidates {
        match read_store_snapshot(store.as_ref(), path).await {
            Ok(payload) => snapshots.push((path.clone(), payload)),
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                warn!(path = %path, error = %error, "ignoring invalid cache snapshot");
                failures.push(error.to_string());
            }
            Err(error) => return Err(error),
        }
    }
    if snapshots.is_empty() && !candidates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no valid snapshots found: {}", failures.join("; ")),
        ));
    }
    snapshots.sort_by_key(|(_, payload)| payload.created_at);
    Ok(snapshots.pop())
}

async fn retain_store_snapshots(
    store: &dyn ObjectStore,
    prefix: &str,
    retention: usize,
) -> io::Result<()> {
    let paths = snapshot_paths(store, prefix).await?;
    let mut snapshots = Vec::new();
    for path in paths {
        if let Ok(payload) = read_store_snapshot(store, &path).await {
            snapshots.push((payload.created_at, path));
        }
    }
    snapshots.sort_by_key(|(created_at, _)| *created_at);
    let remove_count = snapshots.len().saturating_sub(retention);
    for (_, path) in snapshots.into_iter().take(remove_count) {
        store
            .delete(&path)
            .await
            .map_err(|error| io::Error::other(format!("delete old snapshot {path}: {error}")))?;
    }
    Ok(())
}

fn payload_from_entries(entries: Vec<PersistenceEntry>) -> io::Result<SnapshotPayload> {
    let created_at = Timestamp::from_system_time(SystemTime::now())?;
    let mut entries: Vec<_> = entries
        .into_iter()
        .map(|entry| {
            Ok(SnapshotEntry {
                key: entry.key,
                value: entry.value,
                expires_at: entry
                    .expires_at
                    .map(Timestamp::from_system_time)
                    .transpose()?,
            })
        })
        .collect::<io::Result<_>>()?;
    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    Ok(SnapshotPayload {
        created_at,
        entries,
    })
}

fn encode_snapshot(payload: &SnapshotPayload) -> io::Result<Vec<u8>> {
    let payload_bytes = serde_json::to_vec(payload)
        .map_err(|error| io::Error::other(format!("serialize snapshot: {error}")))?;
    let checksum = crc32(&payload_bytes);
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload_bytes.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes.extend_from_slice(&payload_bytes);
    Ok(bytes)
}

#[cfg(test)]
fn write_snapshot(
    directory: &Path,
    payload: &SnapshotPayload,
    retention: usize,
) -> io::Result<PathBuf> {
    fs::create_dir_all(directory)?;
    let bytes = encode_snapshot(payload)?;
    let file_name = next_snapshot_name(payload.created_at);
    let sequence = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    let final_path = directory.join(file_name);
    let temporary_path = directory.join(format!(".snapshot-{}-{sequence}.tmp", std::process::id()));

    let write_result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary_path, &final_path)?;
        File::open(directory)?.sync_all()?;
        retain_snapshots(directory, retention)?;
        Ok(final_path.clone())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    write_result
}

#[cfg(test)]
fn load_latest_snapshot(directory: &Path) -> io::Result<Option<(PathBuf, SnapshotPayload)>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if is_snapshot_path(&path) {
            candidates.push(path);
        } else if path.extension().is_some_and(|extension| extension == "tmp")
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with(".snapshot-")
        {
            if let Err(error) = fs::remove_file(&path) {
                warn!(path = %path.display(), error = %error, "could not remove interrupted snapshot temporary file");
            }
        }
    }

    let mut snapshots = Vec::new();
    let mut failures = Vec::new();
    for path in candidates.iter() {
        match read_snapshot(path) {
            Ok(payload) => snapshots.push((path.clone(), payload)),
            Err(error) => {
                warn!(path = %path.display(), error = %error, "ignoring invalid cache snapshot");
                failures.push(error.to_string());
            }
        }
    }

    if snapshots.is_empty() && !candidates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no valid snapshots found: {}", failures.join("; ")),
        ));
    }
    snapshots.sort_by_key(|(_, payload)| payload.created_at);
    Ok(snapshots.pop())
}

#[cfg(test)]
fn read_snapshot(path: &Path) -> io::Result<SnapshotPayload> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    decode_snapshot(&bytes)
}

fn decode_snapshot(bytes: &[u8]) -> io::Result<SnapshotPayload> {
    if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid snapshot header",
        ));
    }
    let version = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if version != FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported snapshot format version {version}"),
        ));
    }
    let expected_checksum = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
    let payload_bytes = &bytes[HEADER_LEN..];
    if crc32(payload_bytes) != expected_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot checksum mismatch",
        ));
    }
    let payload: SnapshotPayload = serde_json::from_slice(payload_bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("decode snapshot: {error}"),
        )
    })?;
    payload.created_at.to_system_time()?;
    for entry in &payload.entries {
        if let Some(expires_at) = entry.expires_at {
            expires_at.to_system_time()?;
        }
    }
    Ok(payload)
}

#[cfg(test)]
fn retain_snapshots(directory: &Path, retention: usize) -> io::Result<()> {
    let mut snapshots = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if !is_snapshot_path(&path) {
            continue;
        }
        if let Ok(payload) = read_snapshot(&path) {
            snapshots.push((payload.created_at, path));
        }
    }
    snapshots.sort_by_key(|(created_at, _)| *created_at);
    let remove_count = snapshots.len().saturating_sub(retention);
    for (_, path) in snapshots.into_iter().take(remove_count) {
        fs::remove_file(path)?;
    }
    File::open(directory)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
fn is_snapshot_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(SNAPSHOT_PREFIX) && name.ends_with(SNAPSHOT_EXTENSION))
}

impl Timestamp {
    fn from_system_time(time: SystemTime) -> io::Result<Self> {
        let duration = time.duration_since(UNIX_EPOCH).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("timestamp precedes Unix epoch: {error}"),
            )
        })?;
        Ok(Self {
            seconds: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        })
    }

    fn to_system_time(self) -> io::Result<SystemTime> {
        if self.nanos >= 1_000_000_000 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid timestamp nanoseconds",
            ));
        }
        UNIX_EPOCH
            .checked_add(Duration::new(self.seconds, self.nanos))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "timestamp is out of range"))
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut checksum = !0_u32;
    for byte in bytes {
        checksum ^= u32::from(*byte);
        for _ in 0..8 {
            checksum = (checksum >> 1) ^ (0xedb8_8320 & (0_u32.wrapping_sub(checksum & 1)));
        }
    }
    !checksum
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::Arc,
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use nebula_core::CacheStore;
    use nebula_health::{lifecycle::Lifecycle, ComponentHealth, HealthComponent, HealthManager};

    use crate::{config::PersistenceOptions, metrics::Metrics};

    use super::{
        load_latest_snapshot, write_snapshot, PersistenceManager, SnapshotPayload, Timestamp,
    };

    fn test_directory() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nebula-snapshot-test-{}-{unique}",
            std::process::id()
        ))
    }

    fn manager(directory: PathBuf, retention: usize) -> PersistenceManager {
        let options = PersistenceOptions {
            snapshot_dir: directory,
            ..PersistenceOptions::default()
        };
        PersistenceManager::new(
            options,
            Duration::from_secs(60),
            retention,
            HealthManager::new(Lifecycle::new(), false),
            Metrics::new(),
        )
    }

    fn payload(seconds: u64) -> SnapshotPayload {
        SnapshotPayload {
            created_at: Timestamp { seconds, nanos: 0 },
            entries: Vec::new(),
        }
    }

    #[tokio::test]
    async fn creates_and_restores_snapshot_with_absolute_ttl() {
        let directory = test_directory();
        let original = Arc::new(CacheStore::new());
        original.set(b"permanent".to_vec(), b"value".to_vec(), 0);
        original.set(b"expires".to_vec(), b"soon".to_vec(), 1);
        let persistence = manager(directory.clone(), 3);
        persistence.snapshot(Arc::clone(&original)).await.unwrap();

        let restored = Arc::new(CacheStore::new());
        assert!(persistence.restore(Arc::clone(&restored)).await.unwrap());
        assert_eq!(restored.get(b"permanent"), Some(b"value".to_vec()));
        assert_eq!(restored.get(b"expires"), Some(b"soon".to_vec()));

        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let after_expiration = Arc::new(CacheStore::new());
        persistence
            .restore(Arc::clone(&after_expiration))
            .await
            .unwrap();
        assert_eq!(after_expiration.get(b"expires"), None);
        assert_eq!(after_expiration.get(b"permanent"), Some(b"value".to_vec()));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn snapshots_remain_readable_during_concurrent_cache_modifications() {
        let directory = test_directory();
        let store = Arc::new(CacheStore::new());
        let writer_store = Arc::clone(&store);
        let writer = thread::spawn(move || {
            for index in 0..2_000_u32 {
                let key = index.to_be_bytes().to_vec();
                writer_store.set(key.clone(), key.clone(), 0);
                if index % 2 == 0 {
                    writer_store.delete(&key);
                }
            }
        });
        let persistence = manager(directory.clone(), 4);
        for _ in 0..4 {
            persistence.snapshot(Arc::clone(&store)).await.unwrap();
        }
        writer.join().unwrap();

        let restored = Arc::new(CacheStore::new());
        persistence.restore(Arc::clone(&restored)).await.unwrap();
        for (key, value, _) in restored.snapshot() {
            assert_eq!(key, value);
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn falls_back_to_latest_valid_snapshot_and_rejects_all_corrupt_files() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let older = write_snapshot(&directory, &payload(1), 5).unwrap();
        let corrupted = write_snapshot(&directory, &payload(2), 5).unwrap();
        let mut bytes = fs::read(&corrupted).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        fs::write(&corrupted, bytes).unwrap();
        assert_eq!(load_latest_snapshot(&directory).unwrap().unwrap().0, older);

        fs::write(&older, b"incomplete snapshot").unwrap();
        assert!(load_latest_snapshot(&directory).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn ignores_and_removes_interrupted_temporary_files() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let interrupted = directory.join(".snapshot-123-4.tmp");
        fs::write(&interrupted, b"partial snapshot").unwrap();
        let persistence = manager(directory.clone(), 3);
        let restored = Arc::new(CacheStore::new());
        assert!(!persistence.restore(restored).await.unwrap());
        assert!(!interrupted.exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn retention_keeps_the_newest_valid_snapshots() {
        let directory = test_directory();
        for seconds in 1..=5 {
            write_snapshot(&directory, &payload(seconds), 3).unwrap();
        }
        let (latest_path, latest) = load_latest_snapshot(&directory).unwrap().unwrap();
        assert_eq!(latest.created_at.seconds, 5);
        let snapshots = fs::read_dir(&directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "nbs")
            })
            .count();
        assert_eq!(snapshots, 3);
        assert!(latest_path.exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn restore_reports_invalid_snapshot_as_persistence_health_failure() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("snapshot-broken.nbs"), b"broken").unwrap();
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle, false);
        let options = PersistenceOptions {
            snapshot_dir: directory.clone(),
            ..PersistenceOptions::default()
        };
        let persistence = PersistenceManager::new(
            options,
            Duration::from_secs(60),
            3,
            health.clone(),
            Metrics::new(),
        );
        assert!(persistence
            .restore(Arc::new(CacheStore::new()))
            .await
            .is_err());
        assert_eq!(
            health
                .snapshot()
                .components
                .get(&HealthComponent::Persistence("snapshots".to_owned())),
            Some(&ComponentHealth::Unhealthy(
                "no valid snapshots found: invalid snapshot header".to_owned()
            ))
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
