use std::sync::Arc;

use nebula_node::{config::Config, engine::CacheStore, rest, server};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_args();
    let listener = TcpListener::bind(config.listen_addr).await?;
    let store = Arc::new(CacheStore::new());

    if config.rest_api {
        let rest_listener = TcpListener::bind(config.rest_listen_addr).await?;
        tokio::try_join!(
            server::run(listener, Arc::clone(&store)),
            rest::run(rest_listener, store)
        )?;
    } else {
        server::run(listener, store).await?;
    }
    Ok(())
}
