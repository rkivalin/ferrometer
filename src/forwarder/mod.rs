use async_trait::async_trait;

use crate::error::Result;
use crate::signal::Metric;

#[async_trait]
pub trait Forwarder: Send + Sync {
    /// Forward a batch of metrics to the remote destination.
    async fn forward(&self, metrics: &[Metric]) -> Result<()>;

    /// Human-readable name for logging.
    fn name(&self) -> &str;
}

#[cfg(feature = "forwarder-otlphttp")]
pub mod otlphttp;
