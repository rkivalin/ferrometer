use std::time::Duration;

use tokio::signal;
use tokio::time::Instant;

use crate::error::Result;
use crate::log::sink::Sink;
use crate::log::source::Source;

/// Drives one Source → Sink pair. Polls the source for batches, hands each
/// to the sink, acks on success, backs off on failure. Cursor state lives in
/// the source, so retries after a send failure re-deliver the same batch
/// without re-reading the underlying log store.
///
/// No ring buffer: the source's underlying storage (e.g. journald) is the
/// durable buffer. During a long sink outage this shipper idles with
/// near-zero memory; when the sink comes back, it catches up by walking
/// forward from the last-acked cursor.
pub struct Shipper {
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    batch_size: usize,
    poll_interval: Duration,
    backoff_initial: Duration,
    backoff_max: Duration,
}

impl Shipper {
    pub fn new(source: Box<dyn Source>, sink: Box<dyn Sink>) -> Self {
        Self {
            source,
            sink,
            batch_size: 1000,
            poll_interval: Duration::from_secs(1),
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

            tracing::trace!("shipper: polling source");
            let batch = tokio::select! {
                b = self.source.peek_batch(self.batch_size) => b?,
                _ = &mut shutdown => break,
            };

            let Some(batch) = batch else {
                tracing::trace!(
                    poll_ms = self.poll_interval.as_millis() as u64,
                    "shipper: no entries, sleeping"
                );
                tokio::select! {
                    _ = tokio::time::sleep(self.poll_interval) => continue,
                    _ = &mut shutdown => break,
                }
            };

            tracing::debug!(
                entries = batch.entries.len(),
                end_cursor = %batch.end_cursor,
                "shipper: sending batch"
            );

            let send_result = tokio::select! {
                r = self.sink.send(&batch) => r,
                _ = &mut shutdown => break,
            };

            match send_result {
                Ok(()) => {
                    tracing::trace!("shipper: send ok, acking cursor");
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
