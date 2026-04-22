use async_trait::async_trait;

use crate::error::Result;
use crate::signal::Metric;

#[async_trait]
pub trait Collector: Send + Sync {
    /// Collect metrics. Called on each interval tick.
    async fn collect(&mut self) -> Result<Vec<Metric>>;

    /// Human-readable name for logging.
    #[allow(dead_code)]
    fn name(&self) -> &str;
}

#[cfg(feature = "collector-unix")]
pub mod unix;
