use async_trait::async_trait;

use crate::error::Result;
use crate::log::source::Batch;

/// A push destination for log batches. Sinks are stateless per call —
/// cursor tracking lives on the source side.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Send the batch. Returns Ok on 2xx, Err on any transport or HTTP
    /// failure. The shipper handles retry/backoff; the sink does not.
    async fn send(&self, batch: &Batch) -> Result<()>;

    fn name(&self) -> &str;
}

#[cfg(feature = "log-sink-loki")]
pub mod loki;
