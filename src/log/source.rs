use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::error::Result;

/// Opaque position identifier returned by the source. For journald this is a
/// journal cursor string; other sources may use offsets or timestamps.
pub type Cursor = String;

/// A single log record returned by a source. Timestamp is the record's
/// wall-clock time; fields are any structured key/value pairs carried with the
/// record (e.g. journald's _SYSTEMD_UNIT, PRIORITY, etc.).
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: SystemTime,
    pub message: String,
    pub fields: BTreeMap<String, String>,
}

/// A batch of log entries plus the cursor of the last entry. The cursor is
/// passed back to `Source::ack` after successful delivery; until then the
/// source keeps the batch cached so retries deliver the same entries.
#[derive(Debug)]
pub struct Batch {
    pub entries: Vec<LogEntry>,
    pub end_cursor: Cursor,
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
    /// data without re-reading the underlying source from scratch.
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
