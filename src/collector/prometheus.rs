use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prometheus_parse::{Sample, Scrape, Value};

use crate::auth;
use crate::collector::Collector;
use crate::collector::label_cache::LabelCache;
use crate::config::PrometheusCollectorConfig;
use crate::error::{Error, Result};
use crate::signal::{Labels, Metric, MetricType};
use crate::tls;

pub struct PrometheusScraper {
    name: String,
    client: reqwest::Client,
    url: String,
    auth_header: Option<String>,
    cache: LabelCache,
    /// Label set for the synthetic scrape-health metrics (`up`, …): the
    /// base labels plus `scraper=<name>`.
    health_labels: Arc<Labels>,
    /// Outcome of the previous tick, for transition-based logging. `None`
    /// before the first scrape.
    last_up: Option<bool>,
    /// Consecutive failed ticks so far; reported on recovery.
    failed_ticks: u64,
}

impl PrometheusScraper {
    pub async fn new(name: &str, config: &PrometheusCollectorConfig) -> Result<Self> {
        let auth_header = auth::resolve_header(&config.auth).await?;

        let builder = tls::configure(reqwest::Client::builder(), &config.tls).await?;
        let client = builder
            .timeout(config.scrape_timeout)
            .build()
            .map_err(|e| Error::Collector(format!("failed to create HTTP client: {e}")))?;

        // Base labels come purely from user config. Nothing is auto-injected
        // — instance/host identity lives in the forwarder's resource
        // attributes per OTel semantic convention.
        let base: Labels = config
            .static_labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let mut cache = LabelCache::new(base);
        let health_labels = cache.intern(
            [("scraper".to_string(), name.to_string())]
                .into_iter()
                .collect(),
        );

        Ok(Self {
            name: name.to_string(),
            client,
            url: config.url.clone(),
            auth_header,
            cache,
            health_labels,
            last_up: None,
            failed_ticks: 0,
        })
    }

    async fn scrape(&self) -> Result<String> {
        let mut req = self.client.get(&self.url);
        if let Some(auth) = &self.auth_header {
            req = req.header("Authorization", auth);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Collector(format!("scrape {}: {e}", self.url)))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(Error::Collector(format!(
                "scrape {}: HTTP {status}",
                self.url
            )));
        }
        resp.text()
            .await
            .map_err(|e| Error::Collector(format!("scrape {}: body read: {e}", self.url)))
    }
}

#[async_trait]
impl Collector for PrometheusScraper {
    /// Scrape and parse the target. Always returns the synthetic scrape
    /// health series — `up` (1/0), `scrape_duration_seconds`,
    /// `scrape_samples_scraped` — as Prometheus itself would, so a down
    /// target is visible as `up == 0` rather than as a silently missing
    /// series. On failure no target samples are emitted for the tick (no
    /// stale last-seen values).
    ///
    /// Failures are logged on state transitions only: `warn` when the
    /// target goes down, `info` when it recovers (with the number of ticks
    /// it was down), `debug` for each repeat while down.
    async fn collect(&mut self) -> Result<Vec<Metric>> {
        let started = Instant::now();
        let outcome = self.scrape_and_parse().await;
        let duration = started.elapsed();

        let (mut metrics, samples) = match outcome {
            Ok((m, samples)) => {
                if self.last_up == Some(false) {
                    tracing::info!(
                        scraper = self.name,
                        url = self.url,
                        failed_ticks = self.failed_ticks,
                        "scrape target recovered"
                    );
                }
                self.last_up = Some(true);
                self.failed_ticks = 0;
                (m, samples)
            }
            Err(e) => {
                self.failed_ticks += 1;
                if self.last_up == Some(false) {
                    tracing::debug!(
                        scraper = self.name,
                        url = self.url,
                        error = %e,
                        failed_ticks = self.failed_ticks,
                        "scrape still failing"
                    );
                } else {
                    tracing::warn!(
                        scraper = self.name,
                        url = self.url,
                        error = %e,
                        "scrape target down"
                    );
                }
                self.last_up = Some(false);
                (Vec::new(), 0)
            }
        };

        let up = self.last_up == Some(true);
        let labels = &self.health_labels;
        metrics.push(Metric::gauge(
            "up",
            if up { 1.0 } else { 0.0 },
            labels.clone(),
        ));
        metrics.push(Metric::gauge(
            "scrape_duration_seconds",
            duration.as_secs_f64(),
            labels.clone(),
        ));
        metrics.push(Metric::gauge(
            "scrape_samples_scraped",
            samples as f64,
            labels.clone(),
        ));
        Ok(metrics)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PrometheusScraper {
    /// Returns the forwarded metrics plus the raw sample count (before the
    /// histogram/summary drop), for `scrape_samples_scraped`.
    async fn scrape_and_parse(&mut self) -> Result<(Vec<Metric>, usize)> {
        let body = self.scrape().await?;

        let lines = body.lines().map(|l| Ok(l.to_string()));
        let scrape = Scrape::parse(lines)
            .map_err(|e| Error::Collector(format!("parse scrape {}: {e}", self.url)))?;

        let samples = scrape.samples.len();
        let mut metrics = Vec::with_capacity(samples + 3);
        for sample in scrape.samples {
            if let Some(m) = self.sample_to_metric(sample) {
                metrics.push(m);
            }
        }
        Ok((metrics, samples))
    }

    fn sample_to_metric(&mut self, sample: Sample) -> Option<Metric> {
        let (value, metric_type) = match sample.value {
            Value::Counter(v) => (v, MetricType::Counter),
            Value::Gauge(v) => (v, MetricType::Gauge),
            Value::Untyped(v) => (v, MetricType::Counter),
            // Histograms and summaries have multi-field values that don't
            // fit ferrometer's Gauge/Counter model cleanly. Drop with a
            // warn-once per scraper to avoid log spam.
            Value::Histogram(_) | Value::Summary(_) => {
                tracing::debug!(
                    scraper = self.name,
                    metric = sample.metric,
                    "dropping unsupported histogram/summary sample"
                );
                return None;
            }
        };

        let mut labels = Labels::new();
        for (k, v) in sample.labels.iter() {
            labels.insert(k.to_string(), v.to_string());
        }
        let interned = self.cache.intern(labels);

        let timestamp = sample
            .timestamp
            .timestamp_nanos_opt()
            .map(|ns| std::time::UNIX_EPOCH + Duration::from_nanos(ns.max(0) as u64))
            .unwrap_or_else(std::time::SystemTime::now);

        Some(Metric {
            name: crate::signal::intern_name(&sample.metric),
            labels: interned,
            value,
            timestamp,
            metric_type,
        })
    }
}
