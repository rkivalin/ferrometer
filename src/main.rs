mod cli;
mod collector;
mod config;
mod error;
mod forwarder;
mod pipeline;
mod signal;

#[cfg(feature = "forwarder-otlphttp")]
mod proto;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let filter = match cli.verbose {
        0 => "ferrometer=info",
        1 => "ferrometer=debug",
        2 => "ferrometer=trace",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .init();

    match cli.command {
        Command::Run => {
            let config = config::Config::load(&cli.config)?;
            pipeline::run(config).await?;
        }
        Command::Validate => {
            let config = config::Config::load(&cli.config)?;
            println!(
                "Configuration is valid. {} collector(s), {} forwarder(s) configured.",
                config.metrics.collectors.len(),
                config.metrics.forwarders.len(),
            );
        }
    }

    Ok(())
}
