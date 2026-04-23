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

                    let logs_handles = spawn_logs(logs_config, instance_name).await?;

                    metrics_handle
                        .await
                        .map_err(|e| anyhow::anyhow!("metrics task: {e}"))??;
                    for h in logs_handles {
                        h.await
                            .map_err(|e| anyhow::anyhow!("logs task: {e}"))??;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await?;
        }
        Command::Validate => {
            let config = config::Config::load(&cli.config)?;
            println!(
                "Configuration is valid. {} collector(s), {} forwarder(s), {} log shipper(s) configured.",
                config.metrics.collectors.len(),
                config.metrics.forwarders.len(),
                config.logs.shippers.len(),
            );
        }
    }

    Ok(())
}

#[cfg(all(feature = "log-source-journald-systemd", feature = "log-sink-loki"))]
async fn spawn_logs(
    logs_config: config::LogsConfig,
    instance_name: String,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<error::Result<()>>>> {
    use std::collections::BTreeMap;

    use config::{LogSinkConfig, LogSourceConfig};
    use log::shipper::Shipper;
    use log::sink::loki::LokiSink;
    use log::source::journald::JournaldSource;
    use log::sink::Sink;
    use log::source::Source;

    let mut handles = Vec::new();
    for (name, shipper_config) in logs_config.shippers {
        let mut extra_static_labels = BTreeMap::new();
        extra_static_labels.insert("instance".to_string(), instance_name.clone());

        let source: Box<dyn Source> = match shipper_config.source {
            LogSourceConfig::Journald(cfg) => {
                Box::new(JournaldSource::new(&name, &cfg, extra_static_labels)?)
            }
        };
        let sink: Box<dyn Sink> = match shipper_config.sink {
            LogSinkConfig::Loki(cfg) => Box::new(LokiSink::new(&name, &cfg).await?),
        };
        let shipper = Shipper::new(source, sink);
        handles.push(tokio::task::spawn_local(shipper.run()));
    }
    Ok(handles)
}

#[cfg(not(all(feature = "log-source-journald-systemd", feature = "log-sink-loki")))]
async fn spawn_logs(
    logs_config: config::LogsConfig,
    _instance_name: String,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<error::Result<()>>>> {
    if !logs_config.shippers.is_empty() {
        tracing::warn!(
            count = logs_config.shippers.len(),
            "log shippers configured but log-source-journald-systemd + log-sink-loki features \
             are not compiled in; log shipping disabled"
        );
    }
    Ok(Vec::new())
}
