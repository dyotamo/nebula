use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: SocketAddr,
}

impl Config {
    pub fn from_args() -> Result<Self, String> {
        let mut args = std::env::args().skip(1);
        let mut listen = "0.0.0.0:7001".to_string();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen" => {
                    listen = args.next().ok_or("--listen requires an address")?;
                }
                "--help" | "-h" => {
                    return Err("Usage: nebula-node [--listen 0.0.0.0:7001]".into());
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        let listen_addr = listen.parse().map_err(|e| format!("invalid listen address: {e}"))?;
        Ok(Self { listen_addr })
    }
}
