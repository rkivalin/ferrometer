mod cli;
mod collector;
mod config;
mod error;
mod forwarder;
#[cfg(all(feature = "log-source-journald-systemd", feature = "log-sink-loki"))]
mod log;
mod pipeline;
mod signal;

#[cfg(feature = "forwarder-otlphttp")]
mod proto;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command};

#[tokio::main(flavor = "current_thread")]
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
            let logs_config = config.logs.clone();
            let instance_name = config.instance.name.clone();

            // LocalSet hosts !Send tasks (e.g. the log shipper with the
            // systemd crate's Journal, which is neither Send nor Sync).
            // Send-bound tasks can still be spawned via tokio::spawn inside.
            let local = tokio::task::LocalSet::new();
            local
                .run_until(async move {
                    let metrics_handle = tokio::spawn(pipeline::run(config));

                    let logs_handle = spawn_logs(logs_config, instance_name).await?;

                    metrics_handle
                        .await
                        .map_err(|e| anyhow::anyhow!("metrics task: {e}"))??;
                    if let Some(h) = logs_handle {
                        h.await
                            .map_err(|e| anyhow::anyhow!("logs task: {e}"))??;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await?;
        }
        Command::Validate => {
            let config = config::Config::load(&cli.config)?;
            let loki = config
                .logs
                .loki
                .as_ref()
                .map(|_| 1)
                .unwrap_or(0);
            println!(
                "Configuration is valid. {} collector(s), {} forwarder(s), {} log shipper(s) configured.",
                config.metrics.collectors.len(),
                config.metrics.forwarders.len(),
                loki,
            );
        }
    }

    Ok(())
}

#[cfg(all(feature = "log-source-journald-systemd", feature = "log-sink-loki"))]
async fn spawn_logs(
    logs_config: config::LogsConfig,
    instance_name: String,
) -> anyhow::Result<Option<tokio::task::JoinHandle<error::Result<()>>>> {
    use log::shipper::Shipper;
    use log::sink::loki::LokiSink;
    use log::source::journald::JournaldSource;

    let Some(loki_config) = logs_config.loki else {
        return Ok(None);
    };

    let source = JournaldSource::new("journald")?;
    let sink = LokiSink::new("loki", &loki_config, &instance_name).await?;
    let shipper = Shipper::new(Box::new(source), Box::new(sink));
    Ok(Some(tokio::task::spawn_local(shipper.run())))
}

#[cfg(not(all(feature = "log-source-journald-systemd", feature = "log-sink-loki")))]
async fn spawn_logs(
    logs_config: config::LogsConfig,
    _instance_name: String,
) -> anyhow::Result<Option<tokio::task::JoinHandle<error::Result<()>>>> {
    if logs_config.loki.is_some() {
        tracing::warn!(
            "[logs.loki] configured but log-source-journald-systemd + log-sink-loki features \
             are not compiled in; log shipping disabled"
        );
    }
    Ok(None)
}
