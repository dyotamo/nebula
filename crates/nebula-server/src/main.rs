//! Binary entry point: load server settings, initialize tracing, and run the application lifecycle.

use nebula_server::{application::Application, config::Config};
use tracing_subscriber::EnvFilter;

/// Resolves configuration before installing logging, then hands service ownership to the runtime.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args()?;
    // A valid RUST_LOG wins; invalid or absent environment filters fall back to configured logging.
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.log_level.clone()))?;
    tracing_subscriber::fmt().with_env_filter(filter).init();

    Application::new(config).run().await
}
