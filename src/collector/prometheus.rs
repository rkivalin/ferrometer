use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use prometheus_parse::{Sample, Scrape, Value};

use crate::collector::Collector;
use crate::collector::label_cache::LabelCache;
use crate::config::PrometheusCollectorConfig;
use crate::error::{Error, Result};
use crate::signal::{Labels, Metric, MetricType};

pub struct PrometheusScraper {
    name: String,
    client: reqwest::Client,
    url: String,
    auth_header: Option<String>,
    cache: LabelCache,
}

impl PrometheusScraper {
    pub async fn new(
        name: &str,
        config: &PrometheusCollectorConfig,
    ) -> Result<Self> {
        let password = match &config.password_file {
            Some(path) => {
                let content = tokio::fs::read_to_string(path).await.map_err(|e| {
                    Error::FileRead {
                        path: path.clone(),
                        source: e,
                    }
                })?;
                Some(content.trim().to_string())
            }
            None => None,
        };

        let auth_header = config.username.as_ref().zip(password).map(|(u, p)| {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
            format!("Basic {encoded}")
        });

        let client = reqwest::Client::builder()
            .timeout(config.scrape_timeout)
            .build()
            .map_err(|e| {
                Error::Collector(format!("failed to create HTTP client: {e}"))
            })?;

        // Base labels come purely from user config. Nothing is auto-injected
        // — instance/host identity lives in the forwarder's resource
        // attributes per OTel semantic convention.
        let base: Labels = config
            .static_labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Self {
            name: name.to_string(),
            client,
            url: config.url.clone(),
            auth_header,
            cache: LabelCache::new(base),
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
    async fn collect(&mut self) -> Result<Vec<Metric>> {
        let body = match self.scrape().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    scraper = self.name,
                    url = self.url,
                    error = %e,
                    "scrape failed; emitting no metrics this tick"
                );
                return Ok(Vec::new());
            }
        };

        let lines = body.lines().map(|l| Ok(l.to_string()));
        let scrape = Scrape::parse(lines).map_err(|e| {
            Error::Collector(format!("parse scrape {}: {e}", self.url))
        })?;

        let mut metrics = Vec::with_capacity(scrape.samples.len());
        for sample in scrape.samples {
            if let Some(m) = self.sample_to_metric(sample) {
                metrics.push(m);
            }
        }
        Ok(metrics)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PrometheusScraper {
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
            .map(|ns| {
                std::time::UNIX_EPOCH + Duration::from_nanos(ns.max(0) as u64)
            })
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
