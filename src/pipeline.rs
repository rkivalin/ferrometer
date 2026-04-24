use std::sync::Arc;
use std::time::Duration;

use tokio::signal;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::collector::Collector;
use crate::config::{CollectorConfig, Config, ForwarderConfig};
use crate::error::Result;
use crate::forwarder::Forwarder;
use crate::signal::Metric;

/// Hard-coded soft deadline: how long a "ready" batch waits for slower
/// collectors to join before being submitted on its own.
const SOFT_DEADLINE: Duration = Duration::from_secs(1);

/// Per-collector scheduler entry. The scheduler is the single owner of
/// all metric collection; each `Scheduled` tracks its own cadence and any
/// in-flight collect() future.
struct Scheduled {
    name: String,
    collector: Arc<Mutex<Box<dyn Collector>>>,
    interval: Duration,
    max_runtime: Duration,
    next_run: Instant,
    /// Set while a collect() is in flight.
    active: Option<JoinHandle<()>>,
}

/// Result handed from a spawned collector task back to the scheduler loop.
struct CollectionResult {
    name: String,
    started_at: Instant,
    metrics: Option<Vec<Metric>>,
}

/// Metrics pending submission, one entry per finished-but-not-yet-emitted
/// collector.
struct Finished {
    metrics: Vec<Metric>,
    soft_deadline_at: Instant,
}

pub async fn run(config: Config) -> Result<()> {
    let instance_name = config.instance.name.clone();

    // Build collectors into Scheduled entries.
    let now = Instant::now();
    let mut scheduleds: Vec<Scheduled> = Vec::new();
    for (name, cc) in &config.metrics.collectors {
        let collector = build_collector(name, cc).await?;
        let (interval, max_runtime) = collector_schedule_params(cc);
        scheduleds.push(Scheduled {
            name: name.clone(),
            collector: Arc::new(Mutex::new(collector)),
            interval,
            max_runtime,
            next_run: now,
            active: None,
        });
    }

    // Build forwarders.
    let mut forwarders: Vec<Arc<dyn Forwarder>> = Vec::new();
    let mut forwarder_tasks: Vec<JoinHandle<Result<()>>> = Vec::new();
    for (name, forwarder_config) in &config.metrics.forwarders {
        let fwd = build_forwarder(name, forwarder_config, &instance_name).await?;
        let run_fwd = fwd.clone();
        forwarder_tasks.push(tokio::spawn(async move { run_fwd.run().await }));
        forwarders.push(fwd);
    }

    tracing::info!(
        collectors = scheduleds.len(),
        forwarders = forwarders.len(),
        "pipeline started"
    );

    scheduler_loop(scheduleds, &forwarders).await?;

    // Signal forwarders to flush and exit, then await their tasks
    for fwd in &forwarders {
        fwd.shutdown();
    }
    for task in forwarder_tasks {
        if let Err(e) = task.await {
            tracing::error!(error = %e, "forwarder task panicked");
        }
    }

    tracing::info!("shutdown complete");
    Ok(())
}

async fn scheduler_loop(
    mut scheduleds: Vec<Scheduled>,
    forwarders: &[Arc<dyn Forwarder>],
) -> Result<()> {
    let mut shutdown = std::pin::pin!(signal::ctrl_c());
    let (result_tx, mut result_rx) = mpsc::channel::<CollectionResult>(32);
    let mut finished: Vec<Finished> = Vec::new();

    loop {
        let now = Instant::now();

        // 1. Start any due, idle collectors.
        for s in &mut scheduleds {
            if s.active.is_none() && s.next_run <= now {
                while s.next_run <= now {
                    s.next_run += s.interval;
                }
                let name = s.name.clone();
                let collector = s.collector.clone();
                let max_runtime = s.max_runtime;
                let tx = result_tx.clone();
                let started_at = now;
                s.active = Some(tokio::spawn(async move {
                    let collect_fut = async {
                        let mut g = collector.lock().await;
                        g.collect().await
                    };
                    let outcome = tokio::time::timeout(max_runtime, collect_fut).await;
                    let metrics = match outcome {
                        Ok(Ok(m)) => {
                            tracing::debug!(collector = %name, count = m.len(), "collected");
                            Some(m)
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(collector = %name, error = %e, "collect failed");
                            None
                        }
                        Err(_) => {
                            tracing::warn!(
                                collector = %name,
                                max_runtime_ms = max_runtime.as_millis() as u64,
                                "collector exceeded max_runtime, aborted"
                            );
                            None
                        }
                    };
                    let _ = tx
                        .send(CollectionResult {
                            name,
                            started_at,
                            metrics,
                        })
                        .await;
                }));
            }
        }

        // 2. If we already have finished data ready to emit, emit immediately.
        //    Two triggers:
        //      - no active collectors left (all done), or
        //      - earliest soft deadline has elapsed
        let any_active = scheduleds.iter().any(|s| s.active.is_some());
        let earliest_soft = finished.iter().map(|f| f.soft_deadline_at).min();
        let ready_to_emit = !finished.is_empty()
            && (!any_active || earliest_soft.is_some_and(|d| d <= now));

        if ready_to_emit {
            submit_batch(&mut finished, forwarders);
            continue;
        }

        // 3. Decide what to wait for.
        let next_due = scheduleds
            .iter()
            .filter(|s| s.active.is_none())
            .map(|s| s.next_run)
            .min();

        // If nothing is active and there are no finished batches and no
        // upcoming due collectors, we're done (no collectors configured?).
        if !any_active && finished.is_empty() && next_due.is_none() {
            return Ok(());
        }

        tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::info!("received shutdown signal");
                // Abort in-flight collectors.
                for s in &mut scheduleds {
                    if let Some(h) = s.active.take() { h.abort(); }
                }
                // Emit whatever we have.
                if !finished.is_empty() {
                    submit_batch(&mut finished, forwarders);
                }
                return Ok(());
            }
            r = result_rx.recv() => {
                if let Some(res) = r {
                    handle_result(res, &mut scheduleds, &mut finished);
                }
            }
            _ = async { tokio::time::sleep_until(earliest_soft.unwrap()).await },
                if earliest_soft.is_some() => {}
            _ = async { tokio::time::sleep_until(next_due.unwrap()).await },
                if next_due.is_some() => {}
        }
    }
}

fn handle_result(
    res: CollectionResult,
    scheduleds: &mut [Scheduled],
    finished: &mut Vec<Finished>,
) {
    if let Some(s) = scheduleds.iter_mut().find(|s| s.name == res.name) {
        s.active = None;
    }
    if let Some(metrics) = res.metrics
        && !metrics.is_empty()
    {
        finished.push(Finished {
            metrics,
            soft_deadline_at: res.started_at + SOFT_DEADLINE,
        });
    }
}

fn submit_batch(finished: &mut Vec<Finished>, forwarders: &[Arc<dyn Forwarder>]) {
    let batch: Vec<Metric> = finished.drain(..).flat_map(|f| f.metrics).collect();
    if batch.is_empty() {
        return;
    }
    tracing::debug!(count = batch.len(), "emitting batch");
    for fwd in forwarders {
        fwd.submit(batch.clone());
    }
}

fn collector_schedule_params(cc: &CollectorConfig) -> (Duration, Duration) {
    match cc {
        CollectorConfig::Unix(c) => (c.interval, c.max_runtime),
        CollectorConfig::Prometheus(c) => (c.interval, c.max_runtime),
    }
}

async fn build_collector(
    name: &str,
    config: &CollectorConfig,
) -> Result<Box<dyn Collector>> {
    match config {
        #[cfg(feature = "collector-unix")]
        CollectorConfig::Unix(c) => {
            Ok(Box::new(crate::collector::unix::UnixCollector::new(name, c)?))
        }
        #[cfg(not(feature = "collector-unix"))]
        CollectorConfig::Unix(_) => Err(crate::error::Error::Config(format!(
            "collector {name}: unix collector not compiled (enable feature 'collector-unix')"
        ))),
        #[cfg(feature = "collector-prometheus")]
        CollectorConfig::Prometheus(c) => Ok(Box::new(
            crate::collector::prometheus::PrometheusScraper::new(name, c).await?,
        )),
        #[cfg(not(feature = "collector-prometheus"))]
        CollectorConfig::Prometheus(_) => Err(crate::error::Error::Config(format!(
            "collector {name}: prometheus scraper not compiled (enable feature 'collector-prometheus')"
        ))),
    }
}

async fn build_forwarder(
    name: &str,
    config: &ForwarderConfig,
    instance_name: &str,
) -> Result<Arc<dyn Forwarder>> {
    match config {
        #[cfg(feature = "forwarder-otlphttp")]
        ForwarderConfig::Otlphttp(c) => Ok(
            crate::forwarder::otlphttp::OtlphttpForwarder::new(name, c, instance_name)
                .await?,
        ),
        #[cfg(not(feature = "forwarder-otlphttp"))]
        ForwarderConfig::Otlphttp(_) => {
            let _ = instance_name;
            Err(crate::error::Error::Config(format!(
                "forwarder {name}: otlphttp forwarder not compiled (enable feature 'forwarder-otlphttp')"
            )))
        }
    }
}
