use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use base64::Engine;
use prost::Message as _;

use crate::config::LokiSinkConfig;
use crate::error::{Error, Result};
use crate::log::sink::Sink;
use crate::log::source::{Batch, LogEntry};

/// Push sink for Grafana Loki using the native protobuf body + snappy
/// block compression. Entries are grouped into streams by a small fixed
/// label vocabulary so cardinality stays bounded; per-entry high-cardinality
/// fields go into structured metadata so they're queryable without creating
/// new streams.
pub struct LokiSink {
    name: String,
    client: reqwest::Client,
    endpoint: String,
    auth_header: Option<String>,
    instance: String,
}

// --- Protobuf message definitions -------------------------------------------
//
// Hand-written Rust types mirroring grafana/loki/pkg/push/push.proto. Only
// the fields ferrometer actually emits are declared here; the rest
// (PushRequest.format, EntryAdapter.parsed) are omitted and will be
// implicit-defaulted by any peer that does care about them.

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
    pub async fn new(name: &str, config: &LokiSinkConfig, instance: &str) -> Result<Self> {
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

        let endpoint = normalize_endpoint(&config.endpoint);

        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::Sink(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            name: name.to_string(),
            client,
            endpoint,
            auth_header,
            instance: instance.to_string(),
        })
    }

    fn entry_labels(&self, entry: &LogEntry) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("job".to_string(), "ferrometer".to_string());
        labels.insert("instance".to_string(), self.instance.clone());
        if let Some(unit) = entry.fields.get("_SYSTEMD_UNIT") {
            labels.insert("unit".to_string(), unit.clone());
        }
        if let Some(priority) = entry.fields.get("PRIORITY") {
            labels.insert("priority".to_string(), priority.clone());
        }
        labels
    }

    fn build_body(&self, batch: &Batch) -> Result<Vec<u8>> {
        // Group entries by label set.
        let mut by_stream: BTreeMap<BTreeMap<String, String>, Vec<EntryAdapter>> =
            BTreeMap::new();
        for entry in &batch.entries {
            let labels = self.entry_labels(entry);
            let metadata = extract_metadata(&entry.fields);
            let duration = entry
                .timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            let timestamp = prost_types::Timestamp {
                seconds: duration.as_secs() as i64,
                nanos: duration.subsec_nanos() as i32,
            };
            let structured_metadata: Vec<LabelPairAdapter> = metadata
                .into_iter()
                .map(|(name, value)| LabelPairAdapter { name, value })
                .collect();
            by_stream.entry(labels).or_default().push(EntryAdapter {
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

/// Journald fields to forward as Loki structured metadata. Keys are
/// normalized (strip leading underscore, lowercase). Everything NOT in this
/// list (or in the label set) is dropped.
const STRUCTURED_METADATA_FIELDS: &[(&str, &str)] = &[
    ("_PID", "pid"),
    ("SYSLOG_IDENTIFIER", "syslog_identifier"),
    ("SYSLOG_FACILITY", "syslog_facility"),
    ("_TRANSPORT", "transport"),
    ("_HOSTNAME", "hostname"),
    ("_BOOT_ID", "boot_id"),
    ("_MACHINE_ID", "machine_id"),
    ("_CMDLINE", "cmdline"),
];

fn extract_metadata(fields: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut meta = BTreeMap::new();
    for (journal_key, loki_key) in STRUCTURED_METADATA_FIELDS {
        if let Some(value) = fields.get(*journal_key) {
            meta.insert((*loki_key).to_string(), value.clone());
        }
    }
    meta
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
    async fn send(&self, batch: &Batch) -> Result<()> {
        if batch.entries.is_empty() {
            return Ok(());
        }

        let body = self.build_body(batch)?;
        tracing::debug!(
            sink = self.name,
            endpoint = self.endpoint,
            entries = batch.entries.len(),
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
                entries = batch.entries.len(),
                "shipped log batch"
            );
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(Error::Sink(format!("HTTP {status}: {text}")))
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn normalize_endpoint(raw: &str) -> String {
    let trimmed = raw.trim_end_matches('/').to_string();
    if trimmed.ends_with("/loki/api/v1/push") {
        trimmed
    } else {
        format!("{trimmed}/loki/api/v1/push")
    }
}
