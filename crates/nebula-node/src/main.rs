use std::sync::Arc;

use nebula_node::{admin_console, config::Config, engine::CacheStore, server};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let (config, admin_console) = Config::from_args()?;
    let listener = TcpListener::bind(config.listen_addr).await?;
    let store = Arc::new(CacheStore::new());
    let frame_limits = config.frame_limits();

    if admin_console.enabled {
        let mut admin_listen_addr = config.admin_listen_addr;
        if let Some(port) = admin_console.port {
            admin_listen_addr.set_port(port);
        }
        let admin_listener = TcpListener::bind(admin_listen_addr).await?;
        tokio::try_join!(
            server::run(listener, Arc::clone(&store), frame_limits),
            admin_console::run(
                admin_listener,
                store,
                frame_limits,
                admin_console.password.expect("validated admin password"),
            )
        )?;
    } else {
        server::run(listener, store, frame_limits).await?;
    }
    Ok(())
}
