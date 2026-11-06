use std::{error::Error, fs::File, io, net::SocketAddr, path::PathBuf};

use clap::Parser;
use nebula_protocol::FrameLimits;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub admin_listen_addr: SocketAddr,
    pub max_key_len: usize,
    pub max_value_len: usize,
    pub admin_console: bool,
    pub admin_password: Option<String>,
    pub admin_console_port: Option<u16>,
}

pub struct AdminConsoleOptions {
    pub enabled: bool,
    pub password: Option<String>,
    pub port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:7001".parse().unwrap(),
            admin_listen_addr: "127.0.0.1:7002".parse().unwrap(),
            max_key_len: FrameLimits::default().max_key_len,
            max_value_len: FrameLimits::default().max_value_len,
            admin_console: false,
            admin_password: None,
            admin_console_port: None,
        }
    }
}

impl Config {
    pub fn from_args() -> Result<(Self, AdminConsoleOptions), Box<dyn Error>> {
        Self::load(Args::parse())
    }

    fn load(args: Args) -> Result<(Self, AdminConsoleOptions), Box<dyn Error>> {
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

        let admin_console = AdminConsoleOptions {
            enabled: config.admin_console,
            password: config.admin_password.clone(),
            port: config.admin_console_port,
        };

        if admin_console.enabled && admin_console.password.as_deref().is_none_or(str::is_empty) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "admin_password is required and must not be empty when admin_console is enabled",
            )
            .into());
        }

        if config.max_key_len > u32::MAX as usize || config.max_value_len > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame limits must not exceed the protocol's length fields",
            )
            .into());
        }

        Ok((config, admin_console))
    }

    pub fn frame_limits(&self) -> FrameLimits {
        FrameLimits {
            max_key_len: self.max_key_len,
            max_value_len: self.max_value_len,
        }
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

    coerce_config_field::<bool>(values, "admin_console")?;
    coerce_config_field::<usize>(values, "max_key_len")?;
    coerce_config_field::<usize>(values, "max_value_len")?;
    coerce_config_field::<u16>(values, "admin_console_port")?;
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
    #[arg(long = "config", value_name = "PATH")]
    config: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use clap::Parser;

    use super::{Args, Config};

    #[test]
    fn defaults_are_applied_without_a_config_file() {
        let (config, admin_console) =
            Config::load(Args::try_parse_from(["nebula-server"]).unwrap()).unwrap();

        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:7001");
        assert_eq!(config.admin_listen_addr.to_string(), "127.0.0.1:7002");
        assert!(!admin_console.enabled);
        assert_eq!(admin_console.password, None);
        assert_eq!(admin_console.port, None);
        assert_eq!(config.max_key_len, 1_024);
        assert_eq!(config.max_value_len, 8 * 1024 * 1024);
    }

    #[test]
    fn loads_all_server_settings_from_yaml() {
        let config: Config = serde_yaml::from_str(
            "listen_addr: 127.0.0.1:7003\nadmin_listen_addr: 127.0.0.1:7004\nmax_key_len: 2048\nmax_value_len: 4096\nadmin_console: true\nadmin_password: secret\nadmin_console_port: 7123\n",
        )
        .unwrap();

        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:7003");
        assert_eq!(config.admin_listen_addr.to_string(), "127.0.0.1:7004");
        assert_eq!(config.max_key_len, 2048);
        assert_eq!(config.max_value_len, 4096);
        assert!(config.admin_console);
        assert_eq!(config.admin_password.as_deref(), Some("secret"));
        assert_eq!(config.admin_console_port, Some(7123));
    }

    #[test]
    fn expands_environment_values_and_defaults_using_config_field_types() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let secret_name = format!("NEBULA_TEST_SECRET_{unique}");
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        let secret = "p@ss: value #1";
        std::env::set_var(&secret_name, secret);
        fs::write(
            &path,
            format!(
                "admin_console: \"${{ENABLED:-true}}\"\nadmin_password: \"${{{secret_name}}}\"\nmax_key_len: \"${{MISSING_LIMIT_{unique}:-2048}}\"\nadmin_console_port: \"${{MISSING_PORT_{unique}:-7123}}\"\n"
            ),
        )
        .unwrap();

        let (config, admin_console) = Config::load(Args {
            config: Some(path.clone()),
        })
        .unwrap();

        std::env::remove_var(secret_name);
        fs::remove_file(path).unwrap();
        assert!(config.admin_console);
        assert_eq!(admin_console.password.as_deref(), Some(secret));
        assert_eq!(config.max_key_len, 2048);
        assert_eq!(admin_console.port, Some(7123));
    }

    #[test]
    fn fails_when_required_environment_variable_is_unset() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("NEBULA_MISSING_{unique}");
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        fs::write(&path, format!("admin_password: \"${{{name}}}\"\n")).unwrap();

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
        fs::write(&path, "max_key_len: 2048\n").unwrap();

        let (config, _) = Config::load(
            Args::try_parse_from(["nebula-server", "--config", path.to_str().unwrap()]).unwrap(),
        )
        .unwrap();

        fs::remove_file(path).unwrap();
        assert_eq!(config.max_key_len, 2048);
        assert_eq!(config.max_value_len, 8 * 1024 * 1024);
    }

    #[test]
    fn rejects_non_config_cli_options() {
        assert!(Args::try_parse_from(["nebula-server", "--listen", "127.0.0.1:7002"]).is_err());
        assert!(Args::try_parse_from(["nebula-server", "--admin-console"]).is_err());
        assert!(Args::try_parse_from(["nebula-server", "--max-key-len", "2048"]).is_err());
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
        fs::write(&path, "admin_console: true\n").unwrap();

        let error = Config::load(Args {
            config: Some(path.clone()),
        })
        .err()
        .unwrap();

        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains("admin_password is required"));
    }

    #[test]
    fn rejects_frame_limits_that_do_not_fit_protocol_fields() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nebula-config-{unique}.yml"));
        fs::write(&path, "max_key_len: 4294967296\n").unwrap();

        let error = Config::load(Args {
            config: Some(path.clone()),
        })
        .err()
        .unwrap();

        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains("protocol's length fields"));
    }
}
