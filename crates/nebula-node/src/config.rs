use std::net::SocketAddr;

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(name = "nebula-node", version, about = "Run a Nebula cache server")]
pub struct Config {
    #[arg(
        long = "listen",
        default_value = "0.0.0.0:7001",
        value_name = "HOST:PORT"
    )]
    pub listen_addr: SocketAddr,
    #[arg(long = "rest-api")]
    pub rest_api: bool,
    #[arg(
        long = "rest-listen",
        default_value = "0.0.0.0:7002",
        value_name = "HOST:PORT"
    )]
    pub rest_listen_addr: SocketAddr,
}

impl Config {
    pub fn from_args() -> Self {
        Self::parse()
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn defaults_listen_address() {
        let config = Config::try_parse_from(["nebula-node"]).unwrap();

        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:7001");
    }

    #[test]
    fn parses_listen_address() {
        let config = Config::try_parse_from(["nebula-node", "--listen", "127.0.0.1:7002"]).unwrap();

        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:7002");
    }

    #[test]
    fn rejects_invalid_listen_address() {
        assert!(Config::try_parse_from(["nebula-node", "--listen", "invalid"]).is_err());
    }

    #[test]
    fn rest_api_is_disabled_by_default() {
        let config = Config::try_parse_from(["nebula-node"]).unwrap();

        assert!(!config.rest_api);
        assert_eq!(config.rest_listen_addr.to_string(), "0.0.0.0:7002");
    }

    #[test]
    fn parses_rest_api_options() {
        let config = Config::try_parse_from([
            "nebula-node",
            "--rest-api",
            "--rest-listen",
            "127.0.0.1:7003",
        ])
        .unwrap();

        assert!(config.rest_api);
        assert_eq!(config.rest_listen_addr.to_string(), "127.0.0.1:7003");
    }
}
