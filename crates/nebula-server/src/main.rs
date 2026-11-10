//! Binary entry point: load server settings, initialize tracing, and run the application lifecycle.

use nebula_server::{application::Application, config::Config};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args()?;
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.log_level.clone()))?;
    tracing_subscriber::fmt().with_env_filter(filter).init();

    Application::new(config).run().await
}
