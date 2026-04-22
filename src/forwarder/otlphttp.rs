use std::collections::BTreeMap;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use flate2::Compression as GzCompression;
use flate2::write::GzEncoder;
use prost::Message;

use crate::config::{Compression, OtlphttpForwarderConfig};
use crate::error::{Error, Result};
use crate::forwarder::Forwarder;
use crate::proto::opentelemetry::proto::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, Gauge, Metric as OtlpMetric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum, metric, number_data_point,
    },
    resource::v1::Resource,
};
use crate::signal::{Metric, MetricType};

pub struct OtlphttpForwarder {
    name: String,
    client: reqwest::Client,
    endpoint: String,
    auth_header: Option<String>,
    compression: Compression,
    retry_max: u32,
    retry_interval: Duration,
}

impl OtlphttpForwarder {
    pub async fn new(name: &str, config: &OtlphttpForwarderConfig) -> Result<Self> {
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

        let endpoint = config.endpoint.trim_end_matches('/').to_string();
        let endpoint = if endpoint.ends_with("/v1/metrics") {
            endpoint
        } else {
            format!("{endpoint}/v1/metrics")
        };

        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| Error::Forwarder(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            name: name.to_string(),
            client,
            endpoint,
            auth_header,
            compression: config.compression.clone(),
            retry_max: config.retry_max,
            retry_interval: config.retry_interval,
        })
    }

    fn metrics_to_otlp(&self, metrics: &[Metric]) -> ExportMetricsServiceRequest {
        // Group metrics by name
        let mut by_name: BTreeMap<&str, Vec<&Metric>> = BTreeMap::new();
        for m in metrics {
            by_name.entry(&m.name).or_default().push(m);
        }

        // Build OTLP metrics
        let mut otlp_metrics = Vec::new();
        for (name, group) in &by_name {
            let metric_type = &group[0].metric_type;
            let data_points: Vec<NumberDataPoint> = group
                .iter()
                .map(|m| NumberDataPoint {
                    attributes: labels_to_attributes(&m.labels),
                    time_unix_nano: system_time_to_nanos(m.timestamp),
                    value: Some(number_data_point::Value::AsDouble(m.value)),
                    ..Default::default()
                })
                .collect();

            let data = match metric_type {
                MetricType::Gauge => metric::Data::Gauge(Gauge { data_points }),
                MetricType::Counter => metric::Data::Sum(Sum {
                    data_points,
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    is_monotonic: true,
                }),
            };

            otlp_metrics.push(OtlpMetric {
                name: (*name).to_string(),
                data: Some(data),
                ..Default::default()
            });
        }

        // Extract instance from first metric's labels for resource
        let instance = metrics
            .first()
            .and_then(|m| m.labels.get("instance"))
            .cloned()
            .unwrap_or_default();

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv(
                        "service.name",
                        &instance,
                    )],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: otlp_metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn encode_body(&self, request: &ExportMetricsServiceRequest) -> Result<(Vec<u8>, bool)> {
        let proto_bytes = request.encode_to_vec();

        match self.compression {
            Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
                encoder.write_all(&proto_bytes).map_err(|e| {
                    Error::Forwarder(format!("gzip compression failed: {e}"))
                })?;
                let compressed = encoder.finish().map_err(|e| {
                    Error::Forwarder(format!("gzip finalize failed: {e}"))
                })?;
                Ok((compressed, true))
            }
            Compression::None => Ok((proto_bytes, false)),
        }
    }
}

#[async_trait]
impl Forwarder for OtlphttpForwarder {
    async fn forward(&self, metrics: &[Metric]) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let request = self.metrics_to_otlp(metrics);
        let (body, is_gzip) = self.encode_body(&request)?;

        let mut last_err = None;
        for attempt in 0..=self.retry_max {
            if attempt > 0 {
                tracing::warn!(
                    forwarder = self.name,
                    attempt,
                    "retrying after failure"
                );
                tokio::time::sleep(self.retry_interval).await;
            }

            let mut req = self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/x-protobuf")
                .body(body.clone());

            if is_gzip {
                req = req.header("Content-Encoding", "gzip");
            }
            if let Some(auth) = &self.auth_header {
                req = req.header("Authorization", auth);
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        tracing::debug!(
                            forwarder = self.name,
                            metrics = metrics.len(),
                            bytes = body.len(),
                            "forwarded metrics"
                        );
                        return Ok(());
                    }

                    let body_text = resp.text().await.unwrap_or_default();

                    if status.is_server_error() {
                        last_err = Some(format!("HTTP {status}: {body_text}"));
                        continue;
                    }

                    // Client error — don't retry
                    return Err(Error::Forwarder(format!("HTTP {status}: {body_text}")));
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    continue;
                }
            }
        }

        Err(Error::Forwarder(format!(
            "all {} retries exhausted: {}",
            self.retry_max,
            last_err.unwrap_or_default()
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        key_strindex: 0,
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn labels_to_attributes(labels: &BTreeMap<String, String>) -> Vec<KeyValue> {
    labels
        .iter()
        .filter(|(k, _)| k.as_str() != "instance")
        .map(|(k, v)| kv(k, v))
        .collect()
}

fn system_time_to_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
