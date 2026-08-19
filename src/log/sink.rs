use async_trait::async_trait;

use crate::error::Result;
use crate::log::source::LogEntry;

/// A push destination for log batches. Sinks are stateless per call —
/// cursor tracking lives on the source side.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Send the entries as one request. Returns Ok on 2xx, Err on any
    /// transport or HTTP failure. The shipper handles retry/backoff; the
    /// sink does not — but it must report a payload-too-large rejection as
    /// `Error::SinkPayloadTooLarge` so the shipper can split the batch
    /// rather than retry it verbatim.
    async fn send(&self, entries: &[LogEntry]) -> Result<()>;

    fn name(&self) -> &str;
}

#[cfg(feature = "log-sink-loki")]
pub mod loki;
