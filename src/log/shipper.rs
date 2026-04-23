use std::time::Duration;

use tokio::signal;
use tokio::time::Instant;

use crate::error::Result;
use crate::log::sink::Sink;
use crate::log::source::Source;

/// Drives one Source → Sink pair. The source internally decides when a
/// batch is "ready" (size limit reached or debounce window elapsed since
/// first entry) and blocks peek_batch until then. On successful delivery
/// the shipper acks the cursor; on failure it waits out the exponential
/// backoff, then retries the same (possibly topped-up) batch.
///
/// No ring buffer — the underlying log store (e.g. journald) is the
/// durable buffer. During a sink outage memory stays flat; on recovery
/// the shipper walks forward from the last acked cursor.
pub struct Shipper {
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    backoff_initial: Duration,
    backoff_max: Duration,
}

impl Shipper {
    pub fn new(source: Box<dyn Source>, sink: Box<dyn Sink>) -> Self {
        Self {
            source,
            sink,
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(300),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut shutdown = std::pin::pin!(signal::ctrl_c());
        let mut backoff = Duration::ZERO;
        let mut backoff_until: Option<Instant> = None;

        tracing::info!(
            source = self.source.name(),
            sink = self.sink.name(),
            "log shipper started"
        );

        loop {
            if let Some(deadline) = backoff_until {
                let now = Instant::now();
                if let Some(wait) = deadline.checked_duration_since(now) {
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = &mut shutdown => break,
                    }
                }
            }

            let batch = tokio::select! {
                b = self.source.peek_batch() => b?,
                _ = &mut shutdown => break,
            };

            let Some(batch) = batch else {
                // Source reports it's exhausted; nothing more to do.
                break;
            };

            let send_result = tokio::select! {
                r = self.sink.send(&batch) => r,
                _ = &mut shutdown => break,
            };

            match send_result {
                Ok(()) => {
                    self.source.ack(&batch.end_cursor).await?;
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
                        source = self.source.name(),
                        sink = self.sink.name(),
                        error = %e,
                        backoff_ms = backoff.as_millis() as u64,
                        "log ship failed"
                    );
                }
            }
        }

        tracing::info!(
            source = self.source.name(),
            sink = self.sink.name(),
            "log shipper stopped"
        );
        Ok(())
    }
}
