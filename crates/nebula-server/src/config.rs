use std::{error::Error, fs::File, io, net::SocketAddr, path::PathBuf};

use clap::Parser;
use nebula_protocol::FrameLimits;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub admin_console: AdminConsoleOptions,
    pub shutdown_timeout_secs: u64,
    pub persistence: PersistenceOptions,
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
    SnapshotWal,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConsoleOptions {
    pub enabled: bool,
    pub password: Option<String>,
    pub listen_addr: SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 7001)),
            admin_console: AdminConsoleOptions::default(),
            shutdown_timeout_secs: 30,
            persistence: PersistenceOptions::default(),
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

impl Default for AdminConsoleOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            password: None,
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 7002)),
        }
    }
}

impl Config {
    pub fn from_args() -> Result<Self, Box<dyn Error>> {
        Self::load(Args::parse())
    }

    fn load(args: Args) -> Result<Self, Box<dyn Error>> {
        let config = if let Some(path) = args.config {
            match serde_yaml::from_reader::<_, Option<serde_yaml::Value>>(File::open(path)?)? {
                Some(mut value) => {
                    expand_environment_variables(&mut value)?;
                    coerce_environment_values(&mut value)?;
                    serde_yaml::from_value(value)?
                }
                None => Self::default(),
            }
        } else {
            Self::default()
        };

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

fn expand_environment_variables(value: &mut serde_yaml::Value) -> Result<(), Box<dyn Error>> {
    match value {
        serde_yaml::Value::String(text) => *text = expand_environment_string(text)?,
        serde_yaml::Value::Sequence(values) => {
            for value in values {
                expand_environment_variables(value)?;
            }
        }
        serde_yaml::Value::Mapping(values) => {
            for value in values.values_mut() {
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

fn coerce_environment_values(value: &mut serde_yaml::Value) -> Result<(), Box<dyn Error>> {
    let serde_yaml::Value::Mapping(values) = value else {
        return Ok(());
    };

    coerce_config_field::<u64>(values, "shutdown_timeout_secs")?;

    let admin_console_key = serde_yaml::Value::String("admin_console".to_owned());
    if let Some(serde_yaml::Value::Mapping(admin_console)) = values.get_mut(&admin_console_key) {
        coerce_config_field::<bool>(admin_console, "enabled")?;
    }

    let persistence_key = serde_yaml::Value::String("persistence".to_owned());
    if let Some(serde_yaml::Value::Mapping(persistence)) = values.get_mut(&persistence_key) {
        coerce_config_field::<u64>(persistence, "snapshot_interval_secs")?;
        coerce_config_field::<usize>(persistence, "snapshot_retention")?;
    }

    Ok(())
}

fn coerce_config_field<T>(
    values: &mut serde_yaml::Mapping,
    name: &str,
) -> Result<(), Box<dyn Error>>
where
    T: std::str::FromStr + serde::Serialize,
{
    let key = serde_yaml::Value::String(name.to_owned());
    if let Some(serde_yaml::Value::String(text)) = values.get(&key) {
        let parsed = text.parse::<T>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid value for config field {name}"),
            )
        })?;
        values.insert(key, serde_yaml::to_value(parsed)?);
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

    use super::{Args, Config, PersistenceMode};

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
        assert_eq!(config.shutdown_timeout_secs, 30);
        assert_eq!(config.persistence.mode, PersistenceMode::Volatile);
        assert_eq!(config.persistence.snapshot_interval_secs, 60);
        assert_eq!(config.persistence.snapshot_retention, 3);
        let frame_limits = config.frame_limits();
        assert_eq!(frame_limits.max_key_len, 1_024);
        assert_eq!(frame_limits.max_value_len, 8 * 1024 * 1024);
    }

    #[test]
    fn loads_all_server_settings_from_yaml() {
        let config: Config = serde_yaml::from_str(
            "listen_addr: 127.0.0.1:7003\nshutdown_timeout_secs: 7\nadmin_console:\n  enabled: true\n  password: secret\n  listen_addr: 127.0.0.1:7004\npersistence:\n  mode: snapshot\n  snapshot_dir: /tmp/nebula-snapshots\n  snapshot_interval_secs: 5\n  snapshot_retention: 4\n",
        )
        .unwrap();

        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:7003");
        assert_eq!(
            config.admin_console.listen_addr.to_string(),
            "127.0.0.1:7004"
        );
        assert!(config.admin_console.enabled);
        assert_eq!(config.admin_console.password.as_deref(), Some("secret"));
        assert_eq!(config.shutdown_timeout_secs, 7);
        assert_eq!(config.persistence.mode, PersistenceMode::Snapshot);
        assert_eq!(
            config.persistence.snapshot_dir,
            std::path::PathBuf::from("/tmp/nebula-snapshots")
        );
        assert_eq!(config.persistence.snapshot_interval_secs, 5);
        assert_eq!(config.persistence.snapshot_retention, 4);
    }

    #[test]
    fn expands_environment_values_and_defaults_using_config_field_types() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let secret_name = format!("NEBULA_TEST_SECRET_{unique}");
        let enabled_name = format!("NEBULA_TEST_ENABLED_{unique}");
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        let secret = "p@ss: value #1";
        std::env::set_var(&secret_name, secret);
        fs::write(
            &path,
            format!(
                "admin_console:\n  enabled: \"${{{enabled_name}:-true}}\"\n  password: \"${{{secret_name}}}\"\npersistence:\n  mode: snapshot\n  snapshot_interval_secs: \"${{MISSING_INTERVAL_{unique}:-7}}\"\n"
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
        assert_eq!(config.persistence.snapshot_interval_secs, 7);
    }

    #[test]
    fn fails_when_required_environment_variable_is_unset() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("NEBULA_MISSING_{unique}");
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        fs::write(
            &path,
            format!("admin_console:\n  password: \"${{{name}}}\"\n"),
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
    fn loads_yaml_file_from_config_argument() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        fs::write(&path, "listen_addr: 127.0.0.1:7003\n").unwrap();

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
        assert!(Args::try_parse_from(["nebula-server", "config.yml"]).is_ok());
        assert!(Args::try_parse_from(["nebula-server", "config.yml", "extra.yml"]).is_err());
        assert!(Args::try_parse_from(["nebula-server", "--config", "config.yml"]).is_err());
    }

    #[test]
    fn rejects_malformed_yaml() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        fs::write(&path, "listen_addr: [unterminated\n").unwrap();

        assert!(Config::load(Args {
            config: Some(path.clone())
        })
        .is_err());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_unknown_yaml_fields() {
        assert!(serde_yaml::from_str::<Config>("unknown_setting: true\n").is_err());
    }

    #[test]
    fn admin_console_requires_a_non_empty_password() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        fs::write(&path, "admin_console:\n  enabled: true\n").unwrap();

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
        assert!(serde_yaml::from_str::<Config>("max_key_len: 2048\n").is_err());
        assert!(serde_yaml::from_str::<Config>("max_value_len: 4096\n").is_err());
    }
}
