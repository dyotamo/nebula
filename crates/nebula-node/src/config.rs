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
}
