use std::time::Duration;

use tokio::signal;
use tokio::time::Instant;

use crate::error::{Error, Result};
use crate::log::sink::Sink;
use crate::log::source::{LogEntry, Source};

/// Drives one Source → Sink pair. The source internally decides when a
/// batch is "ready" (size limit reached or debounce window elapsed since
/// first entry) and blocks peek_batch until then. On successful delivery
/// the shipper acks the cursor; on failure it waits out the exponential
/// backoff, then retries the same (possibly topped-up) batch.
///
/// If the sink rejects a request as too large (`Error::SinkPayloadTooLarge`)
/// retrying verbatim can never succeed, so the shipper instead sends the
/// batch in progressively smaller chunks (halving on each rejection) and
/// acks once every chunk is delivered. A single entry the sink refuses on
/// its own is logged at error level and dropped. Because the source can
/// only ack at batch granularity, a crash mid-split re-sends the already
/// delivered chunks after restart; Loki dedupes identical entries, and the
/// split path is rare by design (the source caps batches by bytes).
///
/// If the sink rejects a request as invalid (`Error::SinkRejected`, HTTP
/// 400) the rejection is likewise permanent — Loki has already ingested the
/// valid entries and dropped the rest — so the shipper logs it at error
/// level with the sink's message and treats the request as delivered.
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
    pub fn new(
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
        backoff_initial: Duration,
        backoff_max: Duration,
    ) -> Self {
        Self {
            source,
            sink,
            backoff_initial,
            backoff_max,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut shutdown = std::pin::pin!(signal::ctrl_c());
        let mut backoff = Duration::ZERO;
        let mut backoff_until: Option<Instant> = None;
        // Split state for the in-flight batch. `sent` entries from the front
        // have been delivered but not yet acked (ack is per batch, by end
        // cursor); `chunk` caps how many entries go into one request. Both
        // reset on ack. Relies on peek_batch only ever appending to the
        // in-flight batch between calls, so a prefix count stays valid.
        let mut sent: usize = 0;
        let mut chunk: usize = usize::MAX;

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

            let total = batch.entries.len();
            let pending = &batch.entries[sent.min(total)..];
            if pending.is_empty() {
                // Everything delivered (or the batch was empty) — ack and
                // start fresh.
                self.source.ack(&batch.end_cursor).await?;
                sent = 0;
                chunk = usize::MAX;
                continue;
            }
            let n = pending.len().min(chunk);
            let slice = &pending[..n];

            let send_result = tokio::select! {
                r = self.sink.send(slice) => r,
                _ = &mut shutdown => break,
            };

            match send_result {
                Ok(()) => {
                    sent += n;
                    backoff = Duration::ZERO;
                    backoff_until = None;
                    if sent >= total {
                        self.source.ack(&batch.end_cursor).await?;
                        sent = 0;
                        chunk = usize::MAX;
                    } else {
                        tracing::debug!(
                            source = self.source.name(),
                            sink = self.sink.name(),
                            sent,
                            total,
                            "shipped partial batch"
                        );
                    }
                }
                Err(Error::SinkPayloadTooLarge(msg)) => {
                    // Retrying verbatim can never succeed; no backoff here.
                    if n == 1 {
                        let entry = &slice[0];
                        tracing::error!(
                            source = self.source.name(),
                            sink = self.sink.name(),
                            error = %msg,
                            approx_bytes = entry.approx_size(),
                            labels = ?entry.labels,
                            message = %truncate(&entry.message, 200),
                            "dropping log entry: sink rejects it as too large on its own"
                        );
                        sent += 1;
                        if sent >= total {
                            self.source.ack(&batch.end_cursor).await?;
                            sent = 0;
                            chunk = usize::MAX;
                        }
                    } else {
                        chunk = n / 2;
                        tracing::warn!(
                            source = self.source.name(),
                            sink = self.sink.name(),
                            error = %msg,
                            entries = n,
                            approx_bytes = approx_size(slice),
                            next_chunk = chunk,
                            "sink rejected batch as too large, splitting"
                        );
                    }
                }
                Err(Error::SinkRejected(msg)) => {
                    tracing::error!(
                        source = self.source.name(),
                        sink = self.sink.name(),
                        error = %msg,
                        entries = n,
                        approx_bytes = approx_size(slice),
                        "sink rejected entries as invalid; dropping them (valid entries in the \
                         request were still ingested)"
                    );
                    sent += n;
                    backoff = Duration::ZERO;
                    backoff_until = None;
                    if sent >= total {
                        self.source.ack(&batch.end_cursor).await?;
                        sent = 0;
                        chunk = usize::MAX;
                    }
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
                        entries = n,
                        approx_bytes = approx_size(slice),
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

fn approx_size(entries: &[LogEntry]) -> usize {
    entries.iter().map(LogEntry::approx_size).sum()
}

/// First `max` chars of `s`, with an ellipsis if anything was cut.
fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::time::SystemTime;

    use async_trait::async_trait;

    use super::*;
    use crate::log::source::Batch;

    fn entry(msg: &str) -> LogEntry {
        LogEntry {
            timestamp: SystemTime::UNIX_EPOCH,
            message: msg.to_string(),
            labels: BTreeMap::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn batch(msgs: &[&str], cursor: &str) -> Batch {
        let mut b = Batch::default();
        for m in msgs {
            b.push(entry(m), cursor.to_string());
        }
        b
    }

    /// Serves a fixed list of batches, then reports exhaustion. Emulates the
    /// real contract: peek returns the same batch until ack.
    struct MockSource {
        batches: Vec<Arc<Batch>>,
        next: usize,
        in_flight: Option<Arc<Batch>>,
        acks: Rc<RefCell<Vec<String>>>,
    }

    #[async_trait(?Send)]
    impl Source for MockSource {
        async fn peek_batch(&mut self) -> Result<Option<Arc<Batch>>> {
            if let Some(b) = &self.in_flight {
                return Ok(Some(b.clone()));
            }
            let Some(b) = self.batches.get(self.next) else {
                return Ok(None);
            };
            self.next += 1;
            self.in_flight = Some(b.clone());
            Ok(Some(b.clone()))
        }
        async fn ack(&mut self, cursor: &String) -> Result<()> {
            self.acks.borrow_mut().push(cursor.clone());
            self.in_flight = None;
            Ok(())
        }
        fn name(&self) -> &str {
            "mock-source"
        }
    }

    /// Rejects any request with more than `max_entries` entries, or any
    /// single entry whose message is longer than `max_line`, as too large;
    /// rejects any request containing a message starting with "bad" as
    /// invalid. Records the message lists of successful sends.
    struct MockSink {
        max_entries: usize,
        max_line: usize,
        sends: Arc<Mutex<Vec<Vec<String>>>>,
        attempts: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn send(&self, entries: &[LogEntry]) -> Result<()> {
            *self.attempts.lock().unwrap() += 1;
            if entries.len() > self.max_entries
                || entries.iter().any(|e| e.message.len() > self.max_line)
            {
                return Err(Error::SinkPayloadTooLarge("HTTP 413".into()));
            }
            if entries.iter().any(|e| e.message.starts_with("bad")) {
                return Err(Error::SinkRejected("HTTP 400".into()));
            }
            self.sends
                .lock()
                .unwrap()
                .push(entries.iter().map(|e| e.message.clone()).collect());
            Ok(())
        }
        fn name(&self) -> &str {
            "mock-sink"
        }
    }

    async fn run(
        batches: Vec<Batch>,
        max_entries: usize,
        max_line: usize,
    ) -> (Vec<Vec<String>>, Vec<String>, usize) {
        let acks = Rc::new(RefCell::new(Vec::new()));
        let sends = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(Mutex::new(0));
        let source = MockSource {
            batches: batches.into_iter().map(Arc::new).collect(),
            next: 0,
            in_flight: None,
            acks: acks.clone(),
        };
        let sink = MockSink {
            max_entries,
            max_line,
            sends: sends.clone(),
            attempts: attempts.clone(),
        };
        let shipper = Shipper::new(
            Box::new(source),
            Box::new(sink),
            Duration::from_millis(1),
            Duration::from_millis(1),
        );
        tokio::task::LocalSet::new()
            .run_until(async { shipper.run().await.unwrap() })
            .await;
        let sends = sends.lock().unwrap().clone();
        let acks = acks.borrow().clone();
        let attempts = *attempts.lock().unwrap();
        (sends, acks, attempts)
    }

    #[tokio::test]
    async fn happy_path_sends_whole_batch_then_acks() {
        let (sends, acks, attempts) = run(
            vec![batch(&["a", "b", "c"], "c1"), batch(&["d"], "c2")],
            100,
            100,
        )
        .await;
        assert_eq!(sends, vec![vec!["a", "b", "c"], vec!["d"]]);
        assert_eq!(acks, vec!["c1", "c2"]);
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn oversized_batch_is_split_and_acked_once_fully_delivered() {
        let msgs = ["a", "b", "c", "d", "e", "f", "g"];
        let (sends, acks, _) = run(vec![batch(&msgs, "c1")], 2, 100).await;
        // 7 → rejected; 3 → rejected; 1,1,1,1,1,1,1 delivered in order.
        let flat: Vec<String> = sends.iter().flatten().cloned().collect();
        assert_eq!(flat, msgs);
        assert!(sends.iter().all(|s| s.len() <= 2));
        assert_eq!(acks, vec!["c1"]);
    }

    #[tokio::test]
    async fn chunk_size_persists_within_batch_and_resets_on_ack() {
        let b1: Vec<&str> = (0..8).map(|_| "x").collect();
        let (sends, acks, attempts) =
            run(vec![batch(&b1, "c1"), batch(&["y"], "c2")], 4, 100).await;
        // Batch 1: 8 rejected, then 4 + 4 delivered (chunk stays at 4).
        // Batch 2: single entry sent in one request (chunk reset).
        assert_eq!(sends.len(), 3);
        assert_eq!(sends[0].len(), 4);
        assert_eq!(sends[1].len(), 4);
        assert_eq!(sends[2], vec!["y"]);
        assert_eq!(acks, vec!["c1", "c2"]);
        assert_eq!(attempts, 4);
    }

    #[tokio::test]
    async fn single_oversized_entry_is_dropped_and_rest_delivered() {
        let (sends, acks, _) = run(vec![batch(&["a", "toolong", "b"], "c1")], 100, 3).await;
        let flat: Vec<String> = sends.iter().flatten().cloned().collect();
        assert_eq!(flat, vec!["a", "b"]);
        assert_eq!(acks, vec!["c1"]);
    }

    #[tokio::test]
    async fn lone_oversized_entry_batch_is_dropped_and_acked() {
        let (sends, acks, _) = run(vec![batch(&["toolong"], "c1")], 100, 3).await;
        assert!(sends.is_empty());
        assert_eq!(acks, vec!["c1"]);
    }

    #[tokio::test]
    async fn rejected_batch_is_dropped_and_acked() {
        let (sends, acks, attempts) = run(
            vec![batch(&["a", "bad", "b"], "c1"), batch(&["c"], "c2")],
            100,
            100,
        )
        .await;
        // The 400 batch is acked without retry or split; shipping continues.
        assert_eq!(sends, vec![vec!["c"]]);
        assert_eq!(acks, vec!["c1", "c2"]);
        assert_eq!(attempts, 2);
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("héllo", 2), "hé…");
        assert_eq!(truncate("hi", 5), "hi");
    }
}
