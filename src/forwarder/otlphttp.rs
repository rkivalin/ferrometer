use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use flate2::Compression as GzCompression;
use flate2::write::GzEncoder;
use prost::Message;
use tokio::sync::Notify;
use tokio::time::Instant;

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

struct BufferState {
    batches: VecDeque<Vec<Metric>>,
    total_metrics: usize,
}

pub struct OtlphttpForwarder {
    name: String,
    client: reqwest::Client,
    endpoint: String,
    auth_header: Option<String>,
    compression: Compression,
    buffer_max_metrics: usize,
    request_max_metrics: usize,
    backoff_initial: Duration,
    backoff_max: Duration,
    state: Mutex<BufferState>,
    notify: Notify,
    shutdown: AtomicBool,
}

impl OtlphttpForwarder {
    pub async fn new(name: &str, config: &OtlphttpForwarderConfig) -> Result<Arc<Self>> {
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

        Ok(Arc::new(Self {
            name: name.to_string(),
            client,
            endpoint,
            auth_header,
            compression: config.compression.clone(),
            buffer_max_metrics: config.buffer_max_metrics,
            request_max_metrics: config.request_max_metrics,
            backoff_initial: config.backoff_initial,
            backoff_max: config.backoff_max,
            state: Mutex::new(BufferState {
                batches: VecDeque::new(),
                total_metrics: 0,
            }),
            notify: Notify::new(),
            shutdown: AtomicBool::new(false),
        }))
    }

    /// Pop batches up to request_max_metrics. First batch is always taken even
    /// if it alone exceeds the cap, to avoid getting stuck. Returns the popped
    /// batches in FIFO order.
    fn pop_for_send(&self) -> Vec<Vec<Metric>> {
        let mut state = self.state.lock().expect("buffer mutex poisoned");
        let mut popped = Vec::new();
        let mut taken = 0usize;
        while let Some(front) = state.batches.front() {
            let size = front.len();
            if taken > 0 && taken + size > self.request_max_metrics {
                break;
            }
            let batch = state.batches.pop_front().unwrap();
            state.total_metrics -= batch.len();
            taken += batch.len();
            popped.push(batch);
            if taken >= self.request_max_metrics {
                break;
            }
        }
        popped
    }

    /// Put popped batches back onto the front of the buffer after a failed
    /// send, preserving their original order. Re-enforces the overall cap.
    fn requeue(&self, popped: Vec<Vec<Metric>>) {
        let mut state = self.state.lock().expect("buffer mutex poisoned");
        for batch in popped.into_iter().rev() {
            state.total_metrics += batch.len();
            state.batches.push_front(batch);
        }
        while state.total_metrics > self.buffer_max_metrics {
            let Some(b) = state.batches.pop_front() else {
                break;
            };
            state.total_metrics -= b.len();
        }
    }

    async fn drain_and_send(&self) -> std::result::Result<usize, String> {
        let popped = self.pop_for_send();
        if popped.is_empty() {
            return Ok(0);
        }

        let refs: Vec<&Metric> = popped.iter().flat_map(|b| b.iter()).collect();
        let total = refs.len();

        let request = metrics_to_otlp(&refs);
        let (body, is_gzip) = match encode_body(&request, &self.compression) {
            Ok(b) => b,
            Err(e) => {
                self.requeue(popped);
                return Err(e.to_string());
            }
        };

        match self.send_once(&body, is_gzip).await {
            Ok(()) => {
                tracing::debug!(
                    forwarder = self.name,
                    metrics = total,
                    bytes = body.len(),
                    "forwarded metrics"
                );
                Ok(total)
            }
            Err(e) => {
                self.requeue(popped);
                Err(e)
            }
        }
    }

    async fn send_once(&self, body: &[u8], is_gzip: bool) -> std::result::Result<(), String> {
        let mut req = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/x-protobuf")
            .body(body.to_vec());

        if is_gzip {
            req = req.header("Content-Encoding", "gzip");
        }
        if let Some(auth) = &self.auth_header {
            req = req.header("Authorization", auth);
        }

        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }

        let body_text = resp.text().await.unwrap_or_default();
        Err(format!("HTTP {status}: {body_text}"))
    }
}

#[async_trait]
impl Forwarder for OtlphttpForwarder {
    fn name(&self) -> &str {
        &self.name
    }

    fn submit(&self, metrics: Vec<Metric>) {
        if metrics.is_empty() {
            return;
        }
        {
            let mut state = self.state.lock().expect("buffer mutex poisoned");
            state.total_metrics += metrics.len();
            state.batches.push_back(metrics);
            while state.total_metrics > self.buffer_max_metrics {
                let Some(b) = state.batches.pop_front() else {
                    break;
                };
                state.total_metrics -= b.len();
            }
        }
        self.notify.notify_one();
    }

    async fn run(&self) -> Result<()> {
        let mut backoff = Duration::ZERO;
        let mut backoff_until: Option<Instant> = None;

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                let _ = self.drain_and_send().await;
                return Ok(());
            }

            let has_data = !self
                .state
                .lock()
                .expect("buffer mutex poisoned")
                .batches
                .is_empty();
            if !has_data {
                self.notify.notified().await;
                continue;
            }

            if let Some(deadline) = backoff_until {
                let now = Instant::now();
                if let Some(wait) = deadline.checked_duration_since(now) {
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = self.notify.notified() => {
                            // Could be shutdown or new data; re-evaluate.
                            continue;
                        }
                    }
                }
            }

            match self.drain_and_send().await {
                Ok(_) => {
                    backoff = Duration::ZERO;
                    backoff_until = None;
                }
                Err(e) => {
                    backoff = if backoff.is_zero() {
                        self.backoff_initial
                    } else {
                        (backoff * 2).min(self.backoff_max)
                    };
                    backoff_until = Some(Instant::now() + backoff);
                    tracing::warn!(
                        forwarder = self.name,
                        error = %e,
                        backoff_ms = backoff.as_millis() as u64,
                        "forward failed"
                    );
                }
            }
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.notify.notify_one();
    }
}

fn metrics_to_otlp(metrics: &[&Metric]) -> ExportMetricsServiceRequest {
    let mut by_name: BTreeMap<&'static str, Vec<&Metric>> = BTreeMap::new();
    for m in metrics {
        by_name.entry(m.name).or_default().push(m);
    }

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
            name: name.to_string(),
            data: Some(data),
            ..Default::default()
        });
    }

    let instance = metrics
        .first()
        .and_then(|m| m.labels.get("instance"))
        .cloned()
        .unwrap_or_default();

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", &instance)],
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

fn encode_body(
    request: &ExportMetricsServiceRequest,
    compression: &Compression,
) -> Result<(Vec<u8>, bool)> {
    let proto_bytes = request.encode_to_vec();
    match compression {
        Compression::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
            encoder
                .write_all(&proto_bytes)
                .map_err(|e| Error::Forwarder(format!("gzip compression failed: {e}")))?;
            let compressed = encoder
                .finish()
                .map_err(|e| Error::Forwarder(format!("gzip finalize failed: {e}")))?;
            Ok((compressed, true))
        }
        Compression::None => Ok((proto_bytes, false)),
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
