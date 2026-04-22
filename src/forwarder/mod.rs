use async_trait::async_trait;

use crate::error::Result;
use crate::signal::Metric;

#[async_trait]
pub trait Forwarder: Send + Sync {
    /// Human-readable name for logging.
    #[allow(dead_code)]
    fn name(&self) -> &str;

    /// Append a batch to the forwarder's internal buffer. Non-blocking; drops
    /// oldest batches on overflow. Must never fail due to network state —
    /// delivery errors are handled inside `run`.
    fn submit(&self, metrics: Vec<Metric>);

    /// Drive the send/retry loop. Spawned once at startup and returns only
    /// after `shutdown` has been signaled and the final flush attempt
    /// completes.
    async fn run(&self) -> Result<()>;

    /// Signal `run` to exit after one final flush attempt.
    fn shutdown(&self);
}

#[cfg(feature = "forwarder-otlphttp")]
pub mod otlphttp;
