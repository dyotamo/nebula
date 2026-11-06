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
        }
    }
}

impl Config {
    pub fn from_args() -> Result<(Self, AdminConsoleOptions), Box<dyn Error>> {
        Self::load(Args::parse())
    }

    fn load(args: Args) -> Result<(Self, AdminConsoleOptions), Box<dyn Error>> {
        let mut config = if let Some(path) = args.config {
            serde_yaml::from_reader::<_, Option<Self>>(File::open(path)?)?.unwrap_or_default()
        } else {
            Self::default()
        };

        if let Some(listen_addr) = args.listen_addr {
            config.listen_addr = listen_addr;
        }

        if let Some(max_key_len) = args.max_key_len {
            config.max_key_len = max_key_len;
        }

        if let Some(max_value_len) = args.max_value_len {
            config.max_value_len = max_value_len;
        }

        let admin_console = AdminConsoleOptions {
            enabled: args.admin_console,
            password: args.admin_password,
            port: args.admin_console_port,
        };

        if admin_console.enabled && admin_console.password.as_deref().is_none_or(str::is_empty) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--admin-password is required and must not be empty when --admin-console is enabled",
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

#[derive(Debug, Parser)]
#[command(name = "nebula-server", version, about = "Run a Nebula cache server")]
struct Args {
    #[arg(long = "config", value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long = "listen", value_name = "HOST:PORT")]
    listen_addr: Option<SocketAddr>,
    #[arg(long = "admin-console")]
    admin_console: bool,
    #[arg(long = "admin-password", value_name = "PASSWORD")]
    admin_password: Option<String>,
    #[arg(long = "admin-console-port", value_name = "PORT")]
    admin_console_port: Option<u16>,
    #[arg(long = "max-key-len", value_name = "BYTES")]
    max_key_len: Option<usize>,
    #[arg(long = "max-value-len", value_name = "BYTES")]
    max_value_len: Option<usize>,
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
    fn parses_cli_overrides() {
        let (config, admin_console) = Config::load(
            Args::try_parse_from([
                "nebula-server",
                "--listen",
                "127.0.0.1:7002",
                "--admin-console",
                "--admin-password",
                "secret",
                "--admin-console-port",
                "7123",
                "--max-key-len",
                "2048",
                "--max-value-len",
                "4096",
            ])
            .unwrap(),
        )
        .unwrap();

        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:7002");
        assert!(admin_console.enabled);
        assert_eq!(admin_console.password.as_deref(), Some("secret"));
        assert_eq!(admin_console.port, Some(7123));
        assert_eq!(config.max_key_len, 2048);
        assert_eq!(config.max_value_len, 4096);
    }

    #[test]
    fn loads_yaml_and_applies_defaults_to_missing_fields() {
        let config: Config = serde_yaml::from_str("max_key_len: 2048\n").unwrap();

        assert_eq!(config.max_key_len, 2048);
        assert_eq!(config.max_value_len, 8 * 1024 * 1024);
        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:7001");
    }

    #[test]
    fn loads_optional_yaml_file_from_config_argument() {
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
    fn rejects_unknown_yaml_fields() {
        assert!(serde_yaml::from_str::<Config>("unknown_setting: true\n").is_err());
        assert!(serde_yaml::from_str::<Config>("admin_console: true\n").is_err());
        assert!(serde_yaml::from_str::<Config>("admin_password: secret\n").is_err());
    }

    #[test]
    fn admin_console_requires_a_non_empty_password() {
        let error =
            Config::load(Args::try_parse_from(["nebula-server", "--admin-console"]).unwrap())
                .err()
                .unwrap();

        assert!(error.to_string().contains("--admin-password is required"));
    }

    #[test]
    fn rejects_frame_limits_that_do_not_fit_protocol_fields() {
        let error = Config::load(
            Args::try_parse_from(["nebula-server", "--max-key-len", "4294967296"]).unwrap(),
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("protocol's length fields"));
    }
}
