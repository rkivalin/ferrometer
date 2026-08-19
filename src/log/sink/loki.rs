use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use prost::Message as _;

use crate::auth;
use crate::config::LokiSinkConfig;
use crate::error::{Error, Result};
use crate::log::sink::Sink;
use crate::log::source::LogEntry;
use crate::tls;

/// Push sink for Grafana Loki using the native protobuf body + snappy
/// block compression. Source-agnostic: ships whatever labels and metadata
/// the source populated on each LogEntry, with no knowledge of where those
/// fields originated.
pub struct LokiSink {
    name: String,
    client: reqwest::Client,
    endpoint: String,
    auth_header: Option<String>,
}

// --- Protobuf message definitions -------------------------------------------
//
// Hand-written Rust types mirroring grafana/loki/pkg/push/push.proto. Only
// the fields ferrometer actually emits are declared here.

#[derive(Clone, PartialEq, prost::Message)]
struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<StreamAdapter>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StreamAdapter {
    /// Prometheus-format label set, e.g. `{job="ferrometer",unit="foo.service"}`.
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<EntryAdapter>,
    /// Deprecated field; Loki ignores it but it's part of the wire schema.
    #[prost(uint64, tag = "3")]
    hash: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<prost_types::Timestamp>,
    #[prost(string, tag = "2")]
    line: String,
    #[prost(message, repeated, tag = "3")]
    structured_metadata: Vec<LabelPairAdapter>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct LabelPairAdapter {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

// ---------------------------------------------------------------------------

impl LokiSink {
    pub async fn new(name: &str, config: &LokiSinkConfig) -> Result<Self> {
        let auth_header = auth::resolve_header(&config.auth).await?;

        let endpoint = normalize_endpoint(&config.endpoint);

        let builder = tls::configure(reqwest::Client::builder(), &config.tls).await?;
        let client = builder
            .build()
            .map_err(|e| Error::Sink(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            name: name.to_string(),
            client,
            endpoint,
            auth_header,
        })
    }

    fn build_body(&self, entries: &[LogEntry]) -> Result<Vec<u8>> {
        // Group entries by stream label set.
        let mut by_stream: BTreeMap<BTreeMap<String, String>, Vec<EntryAdapter>> =
            BTreeMap::new();
        for entry in entries {
            let duration = entry
                .timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            let timestamp = prost_types::Timestamp {
                seconds: duration.as_secs() as i64,
                nanos: duration.subsec_nanos() as i32,
            };
            let structured_metadata: Vec<LabelPairAdapter> = entry
                .metadata
                .iter()
                .map(|(name, value)| LabelPairAdapter {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect();
            by_stream
                .entry(entry.labels.clone())
                .or_default()
                .push(EntryAdapter {
                    timestamp: Some(timestamp),
                    line: entry.message.clone(),
                    structured_metadata,
                });
        }

        let streams: Vec<StreamAdapter> = by_stream
            .into_iter()
            .map(|(labels, entries)| StreamAdapter {
                labels: format_label_string(&labels),
                entries,
                hash: 0,
            })
            .collect();

        let request = PushRequest { streams };
        let proto_bytes = request.encode_to_vec();

        let compressed = snap::raw::Encoder::new()
            .compress_vec(&proto_bytes)
            .map_err(|e| Error::Sink(format!("snappy compress: {e}")))?;

        Ok(compressed)
    }
}

/// Format a label set as a Prometheus-style string: `{k1="v1",k2="v2"}`.
/// Values are escaped per Prometheus rules (backslash, quote, newline).
fn format_label_string(labels: &BTreeMap<String, String>) -> String {
    let mut s = String::with_capacity(32 + labels.len() * 16);
    s.push('{');
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(k);
        s.push_str("=\"");
        for c in v.chars() {
            match c {
                '\\' => s.push_str("\\\\"),
                '"' => s.push_str("\\\""),
                '\n' => s.push_str("\\n"),
                _ => s.push(c),
            }
        }
        s.push('"');
    }
    s.push('}');
    s
}

#[async_trait]
impl Sink for LokiSink {
    async fn send(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let body = self.build_body(entries)?;
        tracing::debug!(
            sink = self.name,
            endpoint = self.endpoint,
            entries = entries.len(),
            body_bytes = body.len(),
            "loki: POST"
        );

        let mut req = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(body);
        if let Some(auth) = &self.auth_header {
            req = req.header("Authorization", auth);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Error::Sink(e.to_string()))?;
        let status = resp.status();
        if status.is_success() {
            tracing::debug!(
                sink = self.name,
                entries = entries.len(),
                "shipped log batch"
            );
            Ok(())
        } else {
            // Loki error bodies end with a newline; trim so the message
            // stays on one line in our own logs.
            let text = resp.text().await.unwrap_or_default();
            let text = text.trim();
            let msg = format!("HTTP {status}: {text}");
            if is_payload_too_large(status, text) {
                Err(Error::SinkPayloadTooLarge(msg))
            } else if status == reqwest::StatusCode::BAD_REQUEST {
                // Loki validates per entry: valid entries are ingested,
                // invalid ones dropped, and the first validation error is
                // returned as 400. Permanent — see Error::SinkRejected.
                Err(Error::SinkRejected(msg))
            } else {
                Err(Error::Sink(msg))
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Whether an error response means the request body itself is over a size
/// limit, so that retrying it verbatim can never succeed. HTTP 413 is the
/// canonical signal (proxies, and Loki's own HTTP server limit); Loki also
/// surfaces its internal gRPC limit as HTTP 500 with a body like
/// `rpc error: code = ResourceExhausted desc = grpc: received message
/// larger than max (N vs. 4194304)`.
fn is_payload_too_large(status: reqwest::StatusCode, body: &str) -> bool {
    if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
        return true;
    }
    let body = body.to_ascii_lowercase();
    [
        "larger than max",
        "too large",
        "too big",
        "exceeds the maximum",
    ]
    .iter()
    .any(|needle| body.contains(needle))
}

fn normalize_endpoint(raw: &str) -> String {
    let trimmed = raw.trim_end_matches('/').to_string();
    if trimmed.ends_with("/loki/api/v1/push") {
        trimmed
    } else {
        format!("{trimmed}/loki/api/v1/push")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn payload_too_large_classification() {
        assert!(is_payload_too_large(StatusCode::PAYLOAD_TOO_LARGE, ""));
        assert!(is_payload_too_large(
            StatusCode::INTERNAL_SERVER_ERROR,
            "rpc error: code = ResourceExhausted desc = grpc: received message larger than max (5242880 vs. 4194304)"
        ));
        assert!(is_payload_too_large(
            StatusCode::BAD_GATEWAY,
            "<html>413 Request Entity Too Large</html>"
        ));
        assert!(!is_payload_too_large(
            StatusCode::INTERNAL_SERVER_ERROR,
            "rpc error: code = Unavailable desc = connection refused"
        ));
        assert!(!is_payload_too_large(
            StatusCode::TOO_MANY_REQUESTS,
            "Ingestion rate limit exceeded"
        ));
        assert!(!is_payload_too_large(
            StatusCode::BAD_REQUEST,
            "entry too far behind"
        ));
    }
}
