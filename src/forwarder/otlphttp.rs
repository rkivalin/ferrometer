use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_compression::tokio::bufread::GzipEncoder;
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use futures_util::stream;
use prost::Message;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::io::{ReaderStream, StreamReader};

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

/// State threaded through the `stream::unfold` closure. Each poll emits one
/// batch as a self-contained `ExportMetricsServiceRequest` with one
/// `ResourceMetrics` entry; protobuf wire format concatenates them so the
/// receiver sees an N-entry list.
struct EncoderState {
    popped: Arc<Vec<Vec<Metric>>>,
    batch_idx: usize,
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
        let total: usize = popped.iter().map(|b| b.len()).sum();

        // Share the popped batches with the body stream. The stream holds
        // one Arc; we keep another so we can reclaim the Vec via try_unwrap
        // after the send completes and requeue on failure.
        let popped = Arc::new(popped);
        let stream_popped = popped.clone();

        // The unfold closure is polled only when reqwest's body sink is ready
        // for more bytes, so encoding happens inline with network writes.
        // If the connection fails, the stream is dropped without ever being
        // polled — zero wasted encoding work.
        let body_stream = stream::unfold(
            EncoderState {
                popped: stream_popped,
                batch_idx: 0,
            },
            |mut state| async move {
                if state.batch_idx >= state.popped.len() {
                    return None;
                }
                // Clone the inner Arc so we can borrow into the Vec while
                // mutating state's cursor.
                let batches = state.popped.clone();
                let batch = &batches[state.batch_idx];
                let request = build_slice_request(batch);
                let bytes = Bytes::from(request.encode_to_vec());
                state.batch_idx += 1;
                Some((Ok::<Bytes, std::io::Error>(bytes), state))
            },
        );

        let is_gzip = matches!(self.compression, Compression::Gzip);

        let mut req = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/x-protobuf");
        if is_gzip {
            req = req.header("Content-Encoding", "gzip");
        }
        if let Some(auth) = &self.auth_header {
            req = req.header("Authorization", auth);
        }

        // Wrap the stream in a gzip adapter if compression is enabled. The
        // adapter chain is entirely lazy: StreamReader pulls from the unfold
        // stream on demand, GzipEncoder compresses incrementally with
        // dictionary state preserved across chunks, ReaderStream hands
        // compressed bytes back to reqwest as they're produced.
        let req = if is_gzip {
            let reader = StreamReader::new(body_stream);
            let gzipped = GzipEncoder::new(reader);
            req.body(reqwest::Body::wrap_stream(ReaderStream::new(gzipped)))
        } else {
            req.body(reqwest::Body::wrap_stream(body_stream))
        };

        let send_result = req.send().await;

        // reqwest has fully consumed (or dropped) the body stream by now,
        // so the stream's Arc clone is released.
        let popped = Arc::try_unwrap(popped)
            .expect("body stream should have released its Arc by now");

        match send_result {
            Ok(resp) if resp.status().is_success() => {
                tracing::debug!(
                    forwarder = self.name,
                    metrics = total,
                    "forwarded metrics"
                );
                Ok(total)
            }
            Ok(resp) => {
                let status = resp.status();
                let body_text = resp.text().await.unwrap_or_default();
                self.requeue(popped);
                Err(format!("HTTP {status}: {body_text}"))
            }
            Err(e) => {
                self.requeue(popped);
                Err(e.to_string())
            }
        }
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

fn build_slice_request(slice: &[Metric]) -> ExportMetricsServiceRequest {
    let mut by_name: BTreeMap<&'static str, Vec<&Metric>> = BTreeMap::new();
    for m in slice {
        by_name.entry(m.name).or_default().push(m);
    }

    let mut otlp_metrics = Vec::with_capacity(by_name.len());
    for (name, group) in by_name {
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

    let instance = slice
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
