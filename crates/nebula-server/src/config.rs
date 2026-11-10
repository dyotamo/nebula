//! Configuration schema, defaults, environment expansion, and validation for the server.
//!
//! Values are loaded from TOML, may contain `${NAME}` or `${NAME:-default}` substitutions, and
//! are validated before runtime services are started. Unknown TOML fields are rejected.

use std::{
    error::Error,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::Parser;
use nebula_core::EvictionPolicy;
use nebula_protocol::FrameLimits;
use serde::Deserialize;

/// Top-level server configuration, populated from TOML or defaults.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Address on which the binary cache protocol listens.
    pub listen_addr: SocketAddr,
    /// Optional HTTP administration console settings.
    pub admin: AdminOptions,
    /// TLS certificate and private-key paths for the protocol listener.
    pub tls: TlsOptions,
    /// Authentication backend and whether protocol authentication is enabled.
    pub security: SecurityOptions,
    /// Maximum time allowed for graceful service shutdown.
    pub shutdown_timeout_secs: u64,
    /// Interval between scans that remove expired cache entries.
    pub ttl_cleanup_interval_secs: u64,
    /// Snapshot persistence backend, interval, and retention settings.
    pub persistence: PersistenceOptions,
    /// Optional Prometheus metrics listener settings.
    pub metrics: MetricsOptions,
    /// Cache capacity, connection concurrency, and eviction policy.
    pub resource_limits: ResourceLimitsOptions,
    /// Default tracing filter used when `RUST_LOG` is not valid or unset.
    pub log_level: String,
}

/// Bounds the in-memory cache and number of concurrent TCP connections.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceLimitsOptions {
    /// Maximum accounted cache bytes before the configured eviction policy applies.
    pub max_cache_size_bytes: usize,
    /// Maximum simultaneously active client connections; excess connections are rejected.
    pub max_connections: usize,
    /// Policy used to make room when the cache reaches its byte limit.
    pub eviction_policy: EvictionPolicy,
}

/// Controls protocol authentication and the database used for identities and tokens.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecurityOptions {
    /// Requires a valid token before cache requests; requires TLS to be enabled as well.
    pub authentication: bool,
    /// Backend connection details for user and token records.
    pub auth: AuthStorageOptions,
}

/// Connection settings for the authentication database.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthStorageOptions {
    /// Database engine used for authentication state.
    pub backend: AuthStorageBackend,
    /// SQLite database filename; `:memory:` selects an in-memory database.
    pub database_path: PathBuf,
    /// PostgreSQL connection URL, required when the PostgreSQL backend is selected.
    pub database_url: Option<String>,
}

/// Supported databases for authentication records.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthStorageBackend {
    /// Local SQLite database.
    #[default]
    Sqlite,
    /// PostgreSQL database (`postgresql` is also accepted in TOML).
    #[serde(alias = "postgresql")]
    Postgres,
}

/// TLS settings for encrypted client connections.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsOptions {
    /// Enables TLS for the cache protocol listener.
    pub enabled: bool,
    /// PEM certificate chain presented to clients.
    pub certificate: Option<PathBuf>,
    /// PEM private key matching the configured certificate.
    pub private_key: Option<PathBuf>,
}

/// Snapshot persistence configuration; persistence is disabled in volatile mode.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistenceOptions {
    /// Selects volatile operation or periodic snapshots with startup recovery.
    pub mode: PersistenceMode,
    /// Object storage implementation used for snapshot files.
    pub snapshot_backend: SnapshotBackend,
    /// Local directory used by the filesystem backend.
    pub snapshot_dir: PathBuf,
    /// S3 bucket name; required when the S3 backend is selected.
    pub snapshot_s3_bucket: Option<String>,
    /// Optional S3 region; otherwise the SDK's environment/default resolution applies.
    pub snapshot_s3_region: Option<String>,
    /// Optional S3-compatible endpoint URL.
    pub snapshot_s3_endpoint: Option<String>,
    /// Optional explicit S3 access key ID; configure together with the secret key.
    pub snapshot_s3_access_key_id: Option<String>,
    /// Optional explicit S3 secret key; configure together with the access key ID.
    pub snapshot_s3_secret_access_key: Option<String>,
    /// Object key prefix applied to snapshots in S3 storage.
    pub snapshot_s3_prefix: String,
    /// Seconds between periodic snapshots.
    pub snapshot_interval_secs: u64,
    /// Number of newest valid snapshots retained after each successful write.
    pub snapshot_retention: usize,
}

/// Storage providers available for cache snapshots.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotBackend {
    /// Local filesystem directory.
    #[default]
    Filesystem,
    /// S3 or an S3-compatible object store.
    S3,
}

/// Controls whether cache contents survive a process restart.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceMode {
    /// Cache contents exist only for the lifetime of this process.
    #[default]
    Volatile,
    /// Restores the newest valid snapshot and writes snapshots periodically and at shutdown.
    Snapshot,
}

/// Settings for the Prometheus metrics endpoint.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsOptions {
    /// Starts the metrics HTTP endpoint when true.
    pub enabled: bool,
    /// Address on which metrics are served.
    pub listen_addr: SocketAddr,
}

/// Settings for the HTTP administration console.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminOptions {
    /// Starts the admin listener when true; authentication must also be enabled.
    pub enabled: bool,
    /// Address on which the admin API and browser console are served.
    pub listen_addr: SocketAddr,
    /// Enables dashboard data in the admin API.
    pub metrics_dashboard: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 7001)),
            admin: AdminOptions::default(),
            tls: TlsOptions::default(),
            security: SecurityOptions::default(),
            shutdown_timeout_secs: 30,
            ttl_cleanup_interval_secs: 3_600,
            persistence: PersistenceOptions::default(),
            metrics: MetricsOptions::default(),
            resource_limits: ResourceLimitsOptions::default(),
            log_level: "info".to_owned(),
        }
    }
}

impl Default for ResourceLimitsOptions {
    fn default() -> Self {
        Self {
            max_cache_size_bytes: 64 * 1024 * 1024,
            max_connections: 1_024,
            eviction_policy: EvictionPolicy::Lru,
        }
    }
}

impl Default for SecurityOptions {
    fn default() -> Self {
        Self {
            authentication: false,
            auth: AuthStorageOptions::default(),
        }
    }
}

impl Default for AuthStorageOptions {
    fn default() -> Self {
        Self {
            backend: AuthStorageBackend::Sqlite,
            database_path: PathBuf::from("nebulad.db"),
            database_url: None,
        }
    }
}

impl Default for TlsOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            certificate: None,
            private_key: None,
        }
    }
}

impl Default for PersistenceOptions {
    fn default() -> Self {
        Self {
            mode: PersistenceMode::Volatile,
            snapshot_backend: SnapshotBackend::Filesystem,
            snapshot_dir: PathBuf::from("snapshots"),
            snapshot_s3_bucket: None,
            snapshot_s3_region: None,
            snapshot_s3_endpoint: None,
            snapshot_s3_access_key_id: None,
            snapshot_s3_secret_access_key: None,
            snapshot_s3_prefix: String::new(),
            snapshot_interval_secs: 60,
            snapshot_retention: 3,
        }
    }
}

impl Default for MetricsOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 7003)),
        }
    }
}

impl Default for AdminOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 7002)),
            metrics_dashboard: true,
        }
    }
}

impl Config {
    /// Parses command-line options and loads the selected TOML configuration file.
    ///
    /// If no path is provided, `nebulad.toml` in the current directory is used.
    pub fn from_args() -> Result<Self, Box<dyn Error>> {
        Self::load(Args::parse())
    }

    fn load(args: Args) -> Result<Self, Box<dyn Error>> {
        Self::load_with_default(args, Path::new("nebulad.toml"))
    }

    fn load_with_default(args: Args, default_config_path: &Path) -> Result<Self, Box<dyn Error>> {
        let path = args
            .config
            .unwrap_or_else(|| default_config_path.to_path_buf());
        let contents = std::fs::read_to_string(&path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to read config file {}: {error}", path.display()),
            )
        })?;
        let config = if contents.trim().is_empty() {
            Self::default()
        } else {
            let mut value = toml::from_str::<toml::Value>(&contents)?;
            expand_environment_variables(&mut value)?;
            coerce_environment_values(&mut value)?;
            value.try_into()?
        };

        if config.resource_limits.max_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resource_limits.max_connections exceeds the supported maximum",
            )
            .into());
        }

        if config.admin.enabled && !config.security.authentication {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "security.authentication must be enabled when admin.enabled is true",
            )
            .into());
        }

        if config.security.authentication && !config.tls.enabled {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tls.enabled must be true when security.authentication is enabled",
            )
            .into());
        }

        if config.security.authentication {
            match config.security.auth.backend {
                AuthStorageBackend::Sqlite
                    if config.security.auth.database_path.as_os_str().is_empty() =>
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "security.auth.database_path must not be empty when SQLite auth storage is selected",
                    )
                    .into());
                }
                AuthStorageBackend::Postgres
                    if config
                        .security
                        .auth
                        .database_url
                        .as_deref()
                        .is_none_or(str::is_empty) =>
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "security.auth.database_url must be set when PostgreSQL auth storage is selected",
                    )
                    .into());
                }
                _ => {}
            }
        }

        if config.tls.enabled
            && (config.tls.certificate.is_none() || config.tls.private_key.is_none())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tls.certificate and tls.private_key are required when tls.enabled is true",
            )
            .into());
        }

        if config.ttl_cleanup_interval_secs == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ttl_cleanup_interval_secs must be greater than zero",
            )
            .into());
        }

        if config.persistence.mode == PersistenceMode::Snapshot
            && (config.persistence.snapshot_interval_secs == 0
                || config.persistence.snapshot_retention == 0
                || (config.persistence.snapshot_backend == SnapshotBackend::Filesystem
                    && config.persistence.snapshot_dir.as_os_str().is_empty())
                || (config.persistence.snapshot_backend == SnapshotBackend::S3
                    && config
                        .persistence
                        .snapshot_s3_bucket
                        .as_deref()
                        .is_none_or(str::is_empty)))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot storage must be configured, and snapshot_interval_secs and snapshot_retention must be greater than zero in snapshot mode",
            )
            .into());
        }

        if config.persistence.snapshot_s3_access_key_id.is_some()
            != config.persistence.snapshot_s3_secret_access_key.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "both S3 access key ID and secret access key must be configured together",
            )
            .into());
        }

        Ok(config)
    }

    /// Returns the protocol frame limits applied by the server.
    pub fn frame_limits(&self) -> FrameLimits {
        FrameLimits::default()
    }
}

// Expand strings recursively so placeholders work in nested tables and arrays as well as top-level values.
fn expand_environment_variables(value: &mut toml::Value) -> Result<(), Box<dyn Error>> {
    match value {
        toml::Value::String(text) => *text = expand_environment_string(text)?,
        toml::Value::Array(values) => {
            for value in values {
                expand_environment_variables(value)?;
            }
        }
        toml::Value::Table(values) => {
            for (_, value) in values.iter_mut() {
                expand_environment_variables(value)?;
            }
        }
        _ => {}
    }

    Ok(())
}

fn expand_environment_string(value: &str) -> Result<String, io::Error> {
    let mut expanded = String::with_capacity(value.len());
    let mut cursor = 0;

    // `cursor` indexes the original input; expanded output can have a different length.
    while let Some(relative_start) = value[cursor..].find("${") {
        let start = cursor + relative_start;
        expanded.push_str(&value[cursor..start]);

        let expression_start = start + 2;
        let relative_end = value[expression_start..].find('}').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "unterminated environment variable expression in config",
            )
        })?;
        let end = expression_start + relative_end;
        let expression = &value[expression_start..end];
        let (name, default) = expression
            .split_once(":-")
            .map_or((expression, None), |(name, default)| (name, Some(default)));

        if !valid_environment_name(name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid environment variable name in config",
            ));
        }

        let variable = std::env::var(name);
        // An unset or empty variable uses the inline fallback when one is supplied.
        let replacement = match variable {
            Ok(value) if !value.is_empty() => value,
            Ok(_) | Err(std::env::VarError::NotPresent) => match default {
                Some(default) => default.to_owned(),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("environment variable {name} is not set"),
                    ));
                }
            },
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("environment variable {name} is not valid UTF-8"),
                ));
            }
        };

        expanded.push_str(&replacement);
        cursor = end + 1;
    }

    expanded.push_str(&value[cursor..]);
    Ok(expanded)
}

fn valid_environment_name(name: &str) -> bool {
    let mut characters = name.chars();
    matches!(characters.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
        && characters.all(|character| matches!(character, '_' | 'a'..='z' | 'A'..='Z' | '0'..='9'))
}

fn coerce_environment_values(value: &mut toml::Value) -> Result<(), Box<dyn Error>> {
    let toml::Value::Table(values) = value else {
        return Ok(());
    };

    coerce_config_field::<u64>(values, "shutdown_timeout_secs")?;
    coerce_config_field::<u64>(values, "ttl_cleanup_interval_secs")?;

    if let Some(toml::Value::Table(tls)) = values.get_mut("tls") {
        coerce_config_field::<bool>(tls, "enabled")?;
    }

    if let Some(toml::Value::Table(security)) = values.get_mut("security") {
        coerce_config_field::<bool>(security, "authentication")?;
    }

    if let Some(toml::Value::Table(admin)) = values.get_mut("admin") {
        coerce_config_field::<bool>(admin, "enabled")?;
        coerce_config_field::<bool>(admin, "metrics_dashboard")?;
    }

    if let Some(toml::Value::Table(metrics)) = values.get_mut("metrics") {
        coerce_config_field::<bool>(metrics, "enabled")?;
    }

    if let Some(toml::Value::Table(persistence)) = values.get_mut("persistence") {
        coerce_config_field::<u64>(persistence, "snapshot_interval_secs")?;
        coerce_config_field::<usize>(persistence, "snapshot_retention")?;
    }

    if let Some(toml::Value::Table(resource_limits)) = values.get_mut("resource_limits") {
        coerce_config_field::<usize>(resource_limits, "max_cache_size_bytes")?;
        coerce_config_field::<usize>(resource_limits, "max_connections")?;
    }

    Ok(())
}

fn coerce_config_field<T>(values: &mut toml::Table, name: &str) -> Result<(), Box<dyn Error>>
where
    T: std::str::FromStr + serde::Serialize,
{
    if let Some(toml::Value::String(text)) = values.get(name) {
        let parsed = text.parse::<T>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid value for config field {name}"),
            )
        })?;
        values.insert(name.to_owned(), toml::Value::try_from(parsed)?);
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[command(name = "nebula-server", version, about = "Run a Nebula cache server")]
struct Args {
    #[arg(value_name = "PATH")]
    config: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use clap::Parser;

    use super::{
        Args, AuthStorageBackend, Config, EvictionPolicy, PersistenceMode, SnapshotBackend,
    };

    #[test]
    fn defaults_are_applied_with_an_empty_config_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_path = std::env::temp_dir().join(format!("nebulad-config-{unique}.toml"));
        fs::write(&config_path, "").unwrap();
        let args = Args::try_parse_from(["nebula-server"]).unwrap();
        let config = Config::load_with_default(args, &config_path).unwrap();
        fs::remove_file(config_path).unwrap();
        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:7001");
        assert_eq!(config.admin.listen_addr.to_string(), "127.0.0.1:7002");
        assert!(!config.admin.enabled);
        assert!(!config.tls.enabled);
        assert_eq!(config.tls.certificate, None);
        assert_eq!(config.tls.private_key, None);
        assert_eq!(config.shutdown_timeout_secs, 30);
        assert_eq!(config.ttl_cleanup_interval_secs, 3_600);
        assert_eq!(config.security.auth.backend, AuthStorageBackend::Sqlite);
        assert_eq!(
            config.security.auth.database_path,
            std::path::PathBuf::from("nebulad.db")
        );
        assert_eq!(config.persistence.mode, PersistenceMode::Volatile);
        assert_eq!(
            config.persistence.snapshot_backend,
            SnapshotBackend::Filesystem
        );
        assert_eq!(config.persistence.snapshot_interval_secs, 60);
        assert_eq!(config.persistence.snapshot_retention, 3);
        assert_eq!(
            config.resource_limits.max_cache_size_bytes,
            64 * 1024 * 1024
        );
        assert_eq!(config.resource_limits.max_connections, 1_024);
        assert_eq!(config.resource_limits.eviction_policy, EvictionPolicy::Lru);
        let frame_limits = config.frame_limits();
        assert_eq!(frame_limits.max_key_len, 1_024);
        assert_eq!(frame_limits.max_value_len, 8 * 1024 * 1024);
    }

    #[test]
    fn parses_s3_snapshot_storage_settings() {
        let config: Config = toml::from_str(
            "[persistence]\nmode = 'snapshot'\nsnapshot_backend = 's3'\nsnapshot_s3_bucket = 'nebula-cache'\nsnapshot_s3_region = 'us-east-1'\nsnapshot_s3_endpoint = 'http://localhost:4566'\nsnapshot_s3_access_key_id = 'test'\nsnapshot_s3_secret_access_key = 'test'\nsnapshot_s3_prefix = 'production/snapshots'\n",
        )
        .unwrap();

        assert_eq!(config.persistence.snapshot_backend, SnapshotBackend::S3);
        assert_eq!(
            config.persistence.snapshot_s3_bucket.as_deref(),
            Some("nebula-cache")
        );
        assert_eq!(
            config.persistence.snapshot_s3_region.as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            config.persistence.snapshot_s3_endpoint.as_deref(),
            Some("http://localhost:4566")
        );
        assert_eq!(
            config.persistence.snapshot_s3_access_key_id.as_deref(),
            Some("test")
        );
        assert_eq!(
            config.persistence.snapshot_s3_secret_access_key.as_deref(),
            Some("test")
        );
        assert_eq!(
            config.persistence.snapshot_s3_prefix,
            "production/snapshots"
        );
    }

    #[test]
    fn parses_postgres_auth_storage_settings() {
        let config: Config = toml::from_str(
            "[security]\nauthentication = true\n\n[security.auth]\nbackend = 'postgresql'\ndatabase_url = 'postgresql://user:password@localhost/nebulacache'\n",
        )
        .unwrap();

        assert_eq!(config.security.auth.backend, AuthStorageBackend::Postgres);
        assert_eq!(
            config.security.auth.database_url.as_deref(),
            Some("postgresql://user:password@localhost/nebulacache")
        );
    }

    #[test]
    fn requires_postgres_url_when_authentication_is_enabled() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-postgres-config-{unique}.toml"));
        fs::write(
            &path,
            "[security]\nauthentication = true\n\n[security.auth]\nbackend = 'postgresql'\n\n[tls]\nenabled = true\ncertificate = 'server.pem'\nprivate_key = 'server-key.pem'\n",
        )
        .unwrap();

        let result = Config::load(Args {
            config: Some(path.clone()),
        });
        fs::remove_file(path).unwrap();

        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("security.auth.database_url must be set"));
    }

    #[test]
    fn errors_when_default_config_file_is_missing() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let missing_config = std::env::temp_dir().join(format!("nebulad-missing-{unique}.toml"));
        let args = Args::try_parse_from(["nebula-server"]).unwrap();

        let error = Config::load_with_default(args, &missing_config)
            .err()
            .unwrap();

        assert!(error.to_string().contains(missing_config.to_str().unwrap()));
    }

    #[test]
    fn loads_nebulad_toml_when_no_config_argument_is_provided() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("nebulad-config-{unique}"));
        fs::create_dir(&directory).unwrap();
        let default_config = directory.join("nebulad.toml");
        fs::write(&default_config, r#"listen_addr = "127.0.0.1:7003""#).unwrap();

        let args = Args::try_parse_from(["nebula-server"]).unwrap();
        let config = Config::load_with_default(args, &default_config).unwrap();

        fs::remove_file(default_config).unwrap();
        fs::remove_dir(directory).unwrap();
        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:7003");
    }

    #[test]
    fn loads_all_server_settings_from_toml() {
        let config: Config = toml::from_str(
            r#"listen_addr = "127.0.0.1:7003"
shutdown_timeout_secs = 7
ttl_cleanup_interval_secs = 120

[admin]
enabled = true
listen_addr = "127.0.0.1:7004"

[security]
authentication = true

[tls]
enabled = true
certificate = "/etc/nebula/server-cert.pem"
private_key = "/etc/nebula/server-key.pem"

[persistence]
mode = "snapshot"
snapshot_dir = "/tmp/nebula-snapshots"
snapshot_interval_secs = 5
snapshot_retention = 4

[resource_limits]
max_cache_size_bytes = 4096
max_connections = 12
eviction_policy = "lfu"
"#,
        )
        .unwrap();

        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:7003");
        assert_eq!(config.admin.listen_addr.to_string(), "127.0.0.1:7004");
        assert!(config.admin.enabled);
        assert!(config.security.authentication);
        assert!(config.tls.enabled);
        assert_eq!(
            config.tls.certificate,
            Some(std::path::PathBuf::from("/etc/nebula/server-cert.pem"))
        );
        assert_eq!(
            config.tls.private_key,
            Some(std::path::PathBuf::from("/etc/nebula/server-key.pem"))
        );
        assert_eq!(config.shutdown_timeout_secs, 7);
        assert_eq!(config.ttl_cleanup_interval_secs, 120);
        assert_eq!(config.persistence.mode, PersistenceMode::Snapshot);
        assert_eq!(
            config.persistence.snapshot_dir,
            std::path::PathBuf::from("/tmp/nebula-snapshots")
        );
        assert_eq!(config.persistence.snapshot_interval_secs, 5);
        assert_eq!(config.persistence.snapshot_retention, 4);
        assert_eq!(config.resource_limits.max_cache_size_bytes, 4096);
        assert_eq!(config.resource_limits.max_connections, 12);
        assert_eq!(config.resource_limits.eviction_policy, EvictionPolicy::Lfu);
    }

    #[test]
    fn rejects_zero_ttl_cleanup_interval() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        fs::write(&path, "ttl_cleanup_interval_secs = 0\n").unwrap();

        let error = Config::load(Args {
            config: Some(path.clone()),
        })
        .err()
        .unwrap();

        fs::remove_file(path).unwrap();
        assert!(error
            .to_string()
            .contains("ttl_cleanup_interval_secs must be greater than zero"));
    }

    #[test]
    fn enabled_tls_requires_certificate_and_private_key_paths() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-tls-config-{unique}.toml"));
        fs::write(&path, "[tls]\nenabled = true\n").unwrap();

        let result = Config::load(Args {
            config: Some(path.clone()),
        });

        fs::remove_file(path).unwrap();
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("tls.certificate"));
    }

    #[test]
    fn expands_environment_values_and_defaults_using_config_field_types() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let enabled_name = format!("NEBULA_TEST_ENABLED_{unique}");
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        fs::write(
            &path,
            format!(
                "[admin]\nenabled = \"${{{enabled_name}:-true}}\"\n\n[security]\nauthentication = \"${{{enabled_name}:-true}}\"\n\n[tls]\nenabled = \"${{{enabled_name}:-true}}\"\ncertificate = \"/tmp/server-cert.pem\"\nprivate_key = \"/tmp/server-key.pem\"\n\n[persistence]\nmode = \"snapshot\"\nsnapshot_interval_secs = \"${{MISSING_INTERVAL_{unique}:-7}}\"\n"
            ),
        )
        .unwrap();

        let config = Config::load(Args {
            config: Some(path.clone()),
        })
        .unwrap();

        fs::remove_file(path).unwrap();
        assert!(config.admin.enabled);
        assert!(config.security.authentication);
        assert!(config.tls.enabled);
        assert_eq!(config.persistence.snapshot_interval_secs, 7);
    }

    #[test]
    fn fails_when_required_environment_variable_is_unset() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("NEBULA_MISSING_{unique}");
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        fs::write(&path, format!("listen_addr = \"${{{name}}}\"\n")).unwrap();

        let error = Config::load(Args {
            config: Some(path.clone()),
        })
        .err()
        .unwrap();

        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains(&name));
    }

    #[test]
    fn loads_toml_file_from_config_argument() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        fs::write(&path, "listen_addr = \"127.0.0.1:7003\"\n").unwrap();

        let config =
            Config::load(Args::try_parse_from(["nebula-server", path.to_str().unwrap()]).unwrap())
                .unwrap();

        fs::remove_file(path).unwrap();
        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:7003");
        assert!(!config.admin.enabled);
        assert_eq!(config.shutdown_timeout_secs, 30);
        assert_eq!(config.frame_limits().max_key_len, 1_024);
        assert_eq!(config.frame_limits().max_value_len, 8 * 1024 * 1024);
    }

    #[test]
    fn accepts_no_config_argument_or_one_path_and_rejects_extra_arguments() {
        assert!(Args::try_parse_from(["nebula-server"]).is_ok());
        assert!(Args::try_parse_from(["nebula-server", "config.toml"]).is_ok());
        assert!(Args::try_parse_from(["nebula-server", "config.toml", "extra.toml"]).is_err());
        assert!(Args::try_parse_from(["nebula-server", "--config", "config.toml"]).is_err());
    }

    #[test]
    fn rejects_malformed_toml() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        fs::write(&path, "listen_addr = [\n").unwrap();

        assert!(Config::load(Args {
            config: Some(path.clone())
        })
        .is_err());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_unknown_toml_fields() {
        assert!(toml::from_str::<Config>("unknown_setting = true\n").is_err());
        assert!(toml::from_str::<Config>("[admin]\npassword = 'secret'\n").is_err());
    }

    #[test]
    fn admin_requires_token_authentication() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        fs::write(&path, "[admin]\nenabled = true\n").unwrap();

        let error = Config::load(Args {
            config: Some(path.clone()),
        })
        .err()
        .unwrap();

        fs::remove_file(path).unwrap();
        assert!(error
            .to_string()
            .contains("security.authentication must be enabled"));
    }

    #[test]
    fn rejects_protocol_limit_overrides() {
        assert!(toml::from_str::<Config>("max_key_len = 2048\n").is_err());
        assert!(toml::from_str::<Config>("max_value_len = 4096\n").is_err());
    }
}
