use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use base64::Engine;
use serde_json::json;

use crate::config::LokiSinkConfig;
use crate::error::{Error, Result};
use crate::log::sink::Sink;
use crate::log::source::{Batch, LogEntry};

/// Push sink for Grafana Loki. Uses the JSON flavor of the push API
/// (application/json at /loki/api/v1/push). Entries are grouped into
/// streams by a small fixed label vocabulary so cardinality stays bounded.
pub struct LokiSink {
    name: String,
    client: reqwest::Client,
    endpoint: String,
    auth_header: Option<String>,
    instance: String,
}

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

    fn build_body(&self, batch: &Batch) -> Vec<u8> {
        // Group entries by label set.
        let mut streams: BTreeMap<BTreeMap<String, String>, Vec<[String; 2]>> =
            BTreeMap::new();
        for entry in &batch.entries {
            let labels = self.entry_labels(entry);
            let ts_ns = entry
                .timestamp
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
                .to_string();
            streams
                .entry(labels)
                .or_default()
                .push([ts_ns, entry.message.clone()]);
        }

        let json = json!({
            "streams": streams.into_iter().map(|(labels, values)| {
                json!({ "stream": labels, "values": values })
            }).collect::<Vec<_>>()
        });

        serde_json::to_vec(&json).expect("json serialization of Loki push body")
    }
}

#[async_trait]
impl Sink for LokiSink {
    async fn send(&self, batch: &Batch) -> Result<()> {
        if batch.entries.is_empty() {
            return Ok(());
        }

        let body = self.build_body(batch);
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
            .header("Content-Type", "application/json")
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
