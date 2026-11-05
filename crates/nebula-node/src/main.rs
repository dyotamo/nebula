use std::sync::Arc;

use nebula_node::{config::Config, engine::CacheStore, server};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = Config::from_args().map_err(std::io::Error::other)?;
    let listener = TcpListener::bind(config.listen_addr).await?;
    let store = Arc::new(CacheStore::new());

    server::run(listener, store).await?;
    Ok(())
}
