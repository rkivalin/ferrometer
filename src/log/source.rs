use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::error::Result;

/// Opaque position identifier returned by the source. For journald this is a
/// journal cursor string; other sources may use offsets or timestamps.
pub type Cursor = String;

/// A single log record returned by a source. The source is responsible for
/// projecting its native fields into `labels` (low-cardinality, grouped into
/// streams by sinks that have a stream concept) and `metadata` (per-entry,
/// queryable but not stream-generating).
///
/// This split mirrors the universal log shipping abstraction: Loki calls it
/// "labels vs structured metadata"; OTLP calls it "Resource attributes vs
/// LogRecord attributes". Sinks are source-agnostic — they just ship what's
/// in these two maps.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: SystemTime,
    pub message: String,
    pub labels: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
}

impl LogEntry {
    /// Approximate wire size of this entry once encoded by a sink, used by
    /// sources to cap batches by bytes. Counts message, labels and metadata
    /// plus a small per-field overhead; deliberately errs high (labels are
    /// counted per entry even though stream-oriented sinks send them once
    /// per stream) so that a cap derived from it stays safe.
    pub fn approx_size(&self) -> usize {
        const FIELD_OVERHEAD: usize = 4;
        const ENTRY_OVERHEAD: usize = 16; // timestamp + length prefixes
        let kv = |m: &BTreeMap<String, String>| {
            m.iter()
                .map(|(k, v)| k.len() + v.len() + FIELD_OVERHEAD)
                .sum::<usize>()
        };
        ENTRY_OVERHEAD + self.message.len() + kv(&self.labels) + kv(&self.metadata)
    }
}

/// A batch of log entries plus the cursor of the last entry. The cursor is
/// passed back to `Source::ack` after successful delivery; until then the
/// source keeps the batch cached so retries deliver the same entries.
#[derive(Debug, Default)]
pub struct Batch {
    pub entries: Vec<LogEntry>,
    pub end_cursor: Cursor,
    /// Running sum of `LogEntry::approx_size` over `entries`. Maintained by
    /// the source via `push`.
    pub approx_bytes: usize,
}

impl Batch {
    pub fn push(&mut self, entry: LogEntry, cursor: Cursor) {
        self.approx_bytes += entry.approx_size();
        self.entries.push(entry);
        self.end_cursor = cursor;
    }
}

/// A pull source of log entries. Source implementations own their own cursor
/// state (either in memory or persisted to disk) — the shipper drives
/// peek/ack but does not track position itself.
///
/// The trait does not require Send so that implementations backed by
/// single-thread-only FFI handles (e.g. the `systemd` crate's `Journal`) can
/// fit. Shippers that use !Send sources must be spawned via `spawn_local`
/// inside a `LocalSet`.
#[async_trait(?Send)]
pub trait Source {
    /// Block until a batch is ready for delivery and return it. Batch size
    /// and debounce policy are the source's own concern, set at construction.
    /// Subsequent calls without an intervening `ack` return the same batch
    /// (possibly topped up with entries that accumulated during any backoff
    /// between retries) so failed sends can be retried against the same
    /// data without re-reading the underlying source from scratch. A top-up
    /// may only *append*: entries already in the batch keep their order and
    /// positions, because the shipper tracks partial delivery of a split
    /// batch as a prefix length.
    ///
    /// Returns `None` only in cases where the source is exhausted with no
    /// more data forthcoming (not applicable to tailing sources like
    /// journald). Cancellation is handled by the caller via tokio::select!.
    async fn peek_batch(&mut self) -> Result<Option<Arc<Batch>>>;

    /// Advance the cursor past the given position and persist it so a
    /// subsequent process restart resumes from the same place.
    async fn ack(&mut self, cursor: &Cursor) -> Result<()>;

    fn name(&self) -> &str;
}

#[cfg(feature = "log-source-journald-systemd")]
pub mod journald;
