use std::time::Duration;

use tokio::signal;
use tokio::sync::mpsc;

use crate::collector::Collector;
use crate::config::{CollectorConfig, Config, ForwarderConfig};
use crate::error::Result;
use crate::forwarder::Forwarder;
use crate::signal::Metric;

pub async fn run(config: Config) -> Result<()> {
    let instance_name = config.instance.name.clone();
    let (tx, mut rx) = mpsc::channel::<Vec<Metric>>(64);

    // Spawn collector tasks
    for (name, collector_config) in &config.metrics.collectors {
        let collector = build_collector(name, collector_config, &instance_name)?;
        let interval = match collector_config {
            CollectorConfig::Unix(c) => c.interval,
        };
        let tx = tx.clone();
        let collector_name = name.clone();
        tokio::spawn(async move {
            run_collector(collector, interval, tx, &collector_name).await;
        });
    }
    drop(tx);

    // Build forwarders
    let mut forwarders: Vec<Box<dyn Forwarder>> = Vec::new();
    for (name, forwarder_config) in &config.metrics.forwarders {
        forwarders.push(build_forwarder(name, forwarder_config).await?);
    }

    tracing::info!(
        collectors = config.metrics.collectors.len(),
        forwarders = forwarders.len(),
        "pipeline started"
    );

    // Forward loop with graceful shutdown
    let mut shutdown = std::pin::pin!(signal::ctrl_c());
    loop {
        tokio::select! {
            batch = rx.recv() => {
                match batch {
                    Some(metrics) => {
                        for fwd in &forwarders {
                            if let Err(e) = fwd.forward(&metrics).await {
                                tracing::error!(forwarder = fwd.name(), error = %e, "forward failed");
                            }
                        }
                    }
                    None => break,
                }
            }
            _ = &mut shutdown => {
                tracing::info!("received shutdown signal, flushing pending metrics");
                rx.close();
                while let Some(metrics) = rx.recv().await {
                    for fwd in &forwarders {
                        if let Err(e) = fwd.forward(&metrics).await {
                            tracing::error!(forwarder = fwd.name(), error = %e, "flush failed");
                        }
                    }
                }
                break;
            }
        }
    }

    tracing::info!("shutdown complete");
    Ok(())
}

async fn run_collector(
    mut collector: Box<dyn Collector>,
    interval_duration: Duration,
    tx: mpsc::Sender<Vec<Metric>>,
    name: &str,
) {
    let mut interval = tokio::time::interval(interval_duration);
    loop {
        interval.tick().await;
        match collector.collect().await {
            Ok(metrics) => {
                tracing::debug!(collector = name, count = metrics.len(), "collected metrics");
                if tx.send(metrics).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                tracing::error!(collector = name, error = %e, "collection failed");
            }
        }
    }
}

fn build_collector(
    name: &str,
    config: &CollectorConfig,
    instance_name: &str,
) -> Result<Box<dyn Collector>> {
    match config {
        #[cfg(feature = "collector-unix")]
        CollectorConfig::Unix(c) => Ok(Box::new(
            crate::collector::unix::UnixCollector::new(name, c, instance_name)?,
        )),
        #[cfg(not(feature = "collector-unix"))]
        CollectorConfig::Unix(_) => Err(crate::error::Error::Config(format!(
            "collector {name}: unix collector not compiled (enable feature 'collector-unix')"
        ))),
    }
}

async fn build_forwarder(
    name: &str,
    config: &ForwarderConfig,
) -> Result<Box<dyn Forwarder>> {
    match config {
        #[cfg(feature = "forwarder-otlphttp")]
        ForwarderConfig::Otlphttp(c) => Ok(Box::new(
            crate::forwarder::otlphttp::OtlphttpForwarder::new(name, c).await?,
        )),
        #[cfg(not(feature = "forwarder-otlphttp"))]
        ForwarderConfig::Otlphttp(_) => Err(crate::error::Error::Config(format!(
            "forwarder {name}: otlphttp forwarder not compiled (enable feature 'forwarder-otlphttp')"
        ))),
    }
}
