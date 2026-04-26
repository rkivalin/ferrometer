#[cfg(any(
    feature = "forwarder-otlphttp",
    feature = "collector-prometheus",
    feature = "log-sink-loki"
))]
mod auth;
#[cfg(any(
    feature = "forwarder-otlphttp",
    feature = "collector-prometheus",
    feature = "log-sink-loki"
))]
mod tls;
mod cli;
mod collector;
mod config;
mod error;
mod forwarder;
mod journal_log;
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
    // Install ring as the default rustls crypto provider. We link rustls
    // with `ring` (all-Rust, no C toolchain) and `rustls-no-provider` at the
    // reqwest layer, so reqwest's ClientBuilder would otherwise fail at
    // runtime with "no process-level CryptoProvider available". Safe to call
    // multiple times; only the first wins.
    #[cfg(any(
        feature = "forwarder-otlphttp",
        feature = "collector-prometheus",
        feature = "log-sink-loki"
    ))]
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();

    let filter = match cli.verbose {
        0 => "ferrometer=info",
        1 => "ferrometer=debug",
        2 => "ferrometer=trace",
        _ => "trace",
    };
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));
    if journal_log::under_journald() {
        // stderr goes to the journal — drop our timestamps + colors and let
        // journald see priorities via the syslog `<N>` prefix.
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(false)
            .event_format(journal_log::JournalFormat)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

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
            // load_unresolved skips placeholder expansion so admins can
            // validate a config from their shell without STATE_DIRECTORY
            // and CREDENTIALS_DIRECTORY set.
            let config = config::Config::load_unresolved(&cli.config)?;
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
        let shipper = Shipper::new(
            source,
            sink,
            shipper_config.backoff_initial,
            shipper_config.backoff_max,
        );
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
