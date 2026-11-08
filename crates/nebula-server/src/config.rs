use std::{error::Error, io, net::SocketAddr, path::PathBuf};

use clap::Parser;
use nebula_core::EvictionPolicy;
use nebula_protocol::FrameLimits;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub admin_console: AdminConsoleOptions,
    pub tls: TlsOptions,
    pub shutdown_timeout_secs: u64,
    pub ttl_cleanup_interval_secs: u64,
    pub persistence: PersistenceOptions,
    pub metrics: MetricsOptions,
    pub resource_limits: ResourceLimitsOptions,
    pub log_level: String,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceLimitsOptions {
    pub max_cache_size_bytes: usize,
    pub max_connections: usize,
    pub eviction_policy: EvictionPolicy,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsOptions {
    pub enabled: bool,
    pub certificate: Option<PathBuf>,
    pub private_key: Option<PathBuf>,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistenceOptions {
    pub mode: PersistenceMode,
    pub snapshot_dir: PathBuf,
    pub snapshot_interval_secs: u64,
    pub snapshot_retention: usize,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceMode {
    #[default]
    Volatile,
    Snapshot,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsOptions {
    pub enabled: bool,
    pub listen_addr: SocketAddr,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConsoleOptions {
    pub enabled: bool,
    pub password: Option<String>,
    pub listen_addr: SocketAddr,
    pub metrics_dashboard: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 7001)),
            admin_console: AdminConsoleOptions::default(),
            tls: TlsOptions::default(),
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
            snapshot_dir: PathBuf::from("data/snapshots"),
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

impl Default for AdminConsoleOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            password: None,
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 7002)),
            metrics_dashboard: true,
        }
    }
}

impl Config {
    pub fn from_args() -> Result<Self, Box<dyn Error>> {
        Self::load(Args::parse())
    }

    fn load(args: Args) -> Result<Self, Box<dyn Error>> {
        let config = if let Some(path) = args.config {
            let contents = std::fs::read_to_string(path)?;
            if contents.trim().is_empty() {
                Self::default()
            } else {
                let mut value = toml::from_str::<toml::Value>(&contents)?;
                expand_environment_variables(&mut value)?;
                coerce_environment_values(&mut value)?;
                value.try_into()?
            }
        } else {
            Self::default()
        };

        if config.resource_limits.max_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resource_limits.max_connections exceeds the supported maximum",
            )
            .into());
        }

        if config.admin_console.enabled
            && config
                .admin_console
                .password
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "admin_console.password is required and must not be empty when admin_console.enabled is true",
            )
            .into());
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
                || config.persistence.snapshot_dir.as_os_str().is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistence.snapshot_dir must not be empty, and snapshot_interval_secs and snapshot_retention must be greater than zero in snapshot mode",
            )
            .into());
        }

        Ok(config)
    }

    pub fn frame_limits(&self) -> FrameLimits {
        FrameLimits::default()
    }
}

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

    if let Some(toml::Value::Table(admin_console)) = values.get_mut("admin_console") {
        coerce_config_field::<bool>(admin_console, "enabled")?;
        coerce_config_field::<bool>(admin_console, "metrics_dashboard")?;
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

    use super::{Args, Config, EvictionPolicy, PersistenceMode};

    #[test]
    fn defaults_are_applied_without_a_config_file() {
        let args = Args::try_parse_from(["nebula-server"]).unwrap();
        let config = Config::load(args).unwrap();
        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:7001");
        assert_eq!(
            config.admin_console.listen_addr.to_string(),
            "127.0.0.1:7002"
        );
        assert!(!config.admin_console.enabled);
        assert_eq!(config.admin_console.password, None);
        assert!(!config.tls.enabled);
        assert_eq!(config.tls.certificate, None);
        assert_eq!(config.tls.private_key, None);
        assert_eq!(config.shutdown_timeout_secs, 30);
        assert_eq!(config.ttl_cleanup_interval_secs, 3_600);
        assert_eq!(config.persistence.mode, PersistenceMode::Volatile);
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
    fn loads_all_server_settings_from_toml() {
        let config: Config = toml::from_str(
            r#"listen_addr = "127.0.0.1:7003"
shutdown_timeout_secs = 7
ttl_cleanup_interval_secs = 120

[admin_console]
enabled = true
password = "secret"
listen_addr = "127.0.0.1:7004"

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
        assert_eq!(
            config.admin_console.listen_addr.to_string(),
            "127.0.0.1:7004"
        );
        assert!(config.admin_console.enabled);
        assert_eq!(config.admin_console.password.as_deref(), Some("secret"));
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
        let secret_name = format!("NEBULA_TEST_SECRET_{unique}");
        let enabled_name = format!("NEBULA_TEST_ENABLED_{unique}");
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        let secret = "p@ss: value #1";
        std::env::set_var(&secret_name, secret);
        fs::write(
            &path,
            format!(
                "[admin_console]\nenabled = \"${{{enabled_name}:-true}}\"\npassword = \"${{{secret_name}}}\"\n\n[tls]\nenabled = \"${{{enabled_name}:-true}}\"\ncertificate = \"/tmp/server-cert.pem\"\nprivate_key = \"/tmp/server-key.pem\"\n\n[persistence]\nmode = \"snapshot\"\nsnapshot_interval_secs = \"${{MISSING_INTERVAL_{unique}:-7}}\"\n"
            ),
        )
        .unwrap();

        let config = Config::load(Args {
            config: Some(path.clone()),
        })
        .unwrap();

        std::env::remove_var(secret_name);
        fs::remove_file(path).unwrap();
        assert!(config.admin_console.enabled);
        assert_eq!(config.admin_console.password.as_deref(), Some(secret));
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
        fs::write(
            &path,
            format!("[admin_console]\npassword = \"${{{name}}}\"\n"),
        )
        .unwrap();

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
        assert!(!config.admin_console.enabled);
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
    }

    #[test]
    fn admin_console_requires_a_non_empty_password() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.toml"));
        fs::write(&path, "[admin_console]\nenabled = true\n").unwrap();

        let error = Config::load(Args {
            config: Some(path.clone()),
        })
        .err()
        .unwrap();

        fs::remove_file(path).unwrap();
        assert!(error
            .to_string()
            .contains("admin_console.password is required"));
    }

    #[test]
    fn rejects_protocol_limit_overrides() {
        assert!(toml::from_str::<Config>("max_key_len = 2048\n").is_err());
        assert!(toml::from_str::<Config>("max_value_len = 4096\n").is_err());
    }
}
