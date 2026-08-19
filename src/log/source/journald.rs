use std::collections::BTreeMap;
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use systemd::journal::{Journal, JournalRecord, OpenOptions};
use tokio::io::unix::AsyncFd;
use tokio::time::Instant;

use crate::config::JournaldSourceConfig;
use crate::error::{Error, Result};
use crate::log::source::{Batch, Cursor, LogEntry, Source};

/// Thin wrapper around a raw fd that implements AsRawFd but has no Drop,
/// so tokio's AsyncFd does not close the fd when the Source is dropped —
/// the fd is owned by the Journal and libsystemd closes it on Journal drop.
struct JournalFdRef(RawFd);

impl AsRawFd for JournalFdRef {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

pub struct JournaldSource {
    name: String,
    /// async_fd is declared before journal so it drops first. On Source drop
    /// we want tokio to deregister the fd from the epoll set before libsystemd
    /// closes the fd in Journal's Drop.
    async_fd: AsyncFd<JournalFdRef>,
    journal: Journal,
    cursor_file: PathBuf,
    last_acked: Option<Cursor>,
    /// In-flight batch. Set when a batch is handed to the caller; cleared on
    /// ack. On peek during retry, we try_unwrap this Arc (safe because the
    /// caller drops its Arc clone between loop iterations), top up with any
    /// entries that accumulated, and re-wrap.
    in_flight: Option<Arc<Batch>>,
    /// Whether the journal has been positioned since the last ack. Before
    /// the first peek, `false` — we seek either to the saved cursor or to
    /// tail.
    positioned: bool,
    batch_size: usize,
    /// Approximate byte cap per batch; 0 disables. See
    /// `JournaldSourceConfig::batch_max_bytes`.
    batch_max_bytes: usize,
    /// An entry already read from the journal that did not fit under the
    /// byte cap of the batch being filled. It is the first entry of the next
    /// batch; holding it here (rather than stepping the journal back) keeps
    /// the journal position monotonic and avoids re-reading the record.
    pending: Option<(LogEntry, Cursor)>,
    batch_wait: Duration,
    /// Mapping journal_field → label_name. Applied per-entry.
    labels_map: BTreeMap<String, String>,
    /// Static labels merged into every LogEntry. Includes entries from config
    /// static_labels plus any extras (e.g. instance) passed at construction.
    static_labels: BTreeMap<String, String>,
    /// Mapping journal_field → metadata_name. Applied per-entry.
    metadata_map: BTreeMap<String, String>,
}

impl JournaldSource {
    pub fn new(
        name: &str,
        config: &JournaldSourceConfig,
        extra_static_labels: BTreeMap<String, String>,
    ) -> Result<Self> {
        let journal = OpenOptions::default()
            .system(true)
            .local_only(true)
            .runtime_only(config.runtime_only)
            .open()
            .map_err(|e| Error::Source(format!("open journal: {e}")))?;

        let fd = journal
            .fd()
            .map_err(|e| Error::Source(format!("journal fd: {e}")))?;
        let async_fd = AsyncFd::new(JournalFdRef(fd))
            .map_err(|e| Error::Source(format!("register journal fd: {e}")))?;

        let cursor_file = config.cursor_file.clone();
        let last_acked = read_saved_cursor(&cursor_file)?;

        // Merge config static_labels with any extras passed from main
        // (typically {"instance": <config.instance.name>}). Extras win on
        // conflict so the caller can always override.
        let mut static_labels = config.static_labels.clone();
        for (k, v) in extra_static_labels {
            static_labels.insert(k, v);
        }

        Ok(Self {
            name: name.to_string(),
            async_fd,
            journal,
            cursor_file,
            last_acked,
            in_flight: None,
            positioned: false,
            batch_size: config.batch_size,
            batch_max_bytes: config.batch_max_bytes,
            pending: None,
            batch_wait: config.batch_wait,
            labels_map: config.labels.clone(),
            static_labels,
            metadata_map: config.metadata.clone(),
        })
    }

    fn position_to_start(&mut self) -> Result<()> {
        match &self.last_acked {
            Some(cursor) => {
                tracing::debug!(cursor = %cursor, "journald: seeking to saved cursor");
                if let Err(e) = self.journal.seek_cursor(cursor.as_str()) {
                    tracing::warn!(error = %e, "saved cursor not found in journal, seeking to tail");
                    self.journal
                        .seek_tail()
                        .map_err(|e| Error::Source(format!("seek_tail: {e}")))?;
                } else {
                    let advanced = self
                        .journal
                        .next()
                        .map_err(|e| Error::Source(format!("next after seek: {e}")))?;
                    tracing::debug!(advanced, "journald: advanced past saved cursor");
                }
            }
            None => {
                tracing::debug!("journald: no saved cursor, seeking to tail");
                self.journal
                    .seek_tail()
                    .map_err(|e| Error::Source(format!("seek_tail: {e}")))?;
                let moved = self
                    .journal
                    .previous()
                    .map_err(|e| Error::Source(format!("previous after seek_tail: {e}")))?;
                tracing::debug!(moved, "journald: seek_tail + previous");
            }
        }
        self.positioned = true;
        Ok(())
    }

    /// Wait for the journal to signal that new entries may be available,
    /// then call process() to consume the notification so next_entry() can
    /// see appended data.
    async fn wait_for_journal_event(&mut self) -> Result<()> {
        let mut guard = self
            .async_fd
            .readable()
            .await
            .map_err(|e| Error::Source(format!("async_fd readable: {e}")))?;
        guard.clear_ready();
        drop(guard);
        let result = self
            .journal
            .process()
            .map_err(|e| Error::Source(format!("process: {e}")))?;
        tracing::trace!(?result, "journald: process() after wakeup");
        Ok(())
    }

    /// Whether `batch` has room for more entries under both the entry-count
    /// and byte caps. Used by every fill path (initial catch-up, coalesce
    /// window, and the retry top-up in `peek_batch`) so a batch can never
    /// grow past the caps no matter how it was assembled.
    fn batch_has_room(&self, batch: &Batch) -> bool {
        batch.entries.len() < self.batch_size
            && (self.batch_max_bytes == 0 || batch.approx_bytes < self.batch_max_bytes)
    }

    /// Read available entries into `batch` until it is full by count
    /// (`batch_size`) or by approximate bytes (`batch_max_bytes`), advancing
    /// the journal cursor. Does not block — returns as soon as next_entry()
    /// returns None.
    ///
    /// The byte cap is enforced *before* an entry is admitted: if the next
    /// entry would push a non-empty batch over the limit it is parked in
    /// `self.pending` and becomes the first entry of the next batch. A
    /// single entry larger than the cap is always admitted into an
    /// otherwise-empty batch — the cap bounds batches, it must not wedge
    /// the source.
    fn drain_available(&mut self, batch: &mut Batch) -> Result<()> {
        while self.batch_has_room(batch) {
            let (entry, cursor) = match self.pending.take() {
                Some(p) => p,
                None => match self
                    .journal
                    .next_entry()
                    .map_err(|e| Error::Source(format!("next_entry: {e}")))?
                {
                    Some(record) => {
                        let cursor = self
                            .journal
                            .cursor()
                            .map_err(|e| Error::Source(format!("cursor: {e}")))?;
                        let timestamp = self
                            .journal
                            .timestamp()
                            .map_err(|e| Error::Source(format!("timestamp: {e}")))?;
                        (self.record_to_entry(record, timestamp), cursor)
                    }
                    None => break,
                },
            };
            if self.batch_max_bytes != 0
                && !batch.entries.is_empty()
                && batch.approx_bytes + entry.approx_size() > self.batch_max_bytes
            {
                self.pending = Some((entry, cursor));
                break;
            }
            batch.push(entry, cursor);
        }
        Ok(())
    }

    fn record_to_entry(&self, mut record: JournalRecord, timestamp: SystemTime) -> LogEntry {
        let message = record.remove("MESSAGE").unwrap_or_default();

        // Start with static labels, overlay dynamic mapping on top so
        // config.labels wins over static_labels when both target the same
        // key (unusual but well-defined).
        let mut labels = self.static_labels.clone();
        for (label_name, journal_key) in &self.labels_map {
            if let Some(value) = record.get(journal_key) {
                labels.insert(label_name.clone(), value.clone());
            }
        }

        let mut metadata = BTreeMap::new();
        for (meta_name, journal_key) in &self.metadata_map {
            if let Some(value) = record.get(journal_key) {
                metadata.insert(meta_name.clone(), value.clone());
            }
        }

        LogEntry {
            timestamp,
            message,
            labels,
            metadata,
        }
    }

    /// Build a fresh batch: drain any entries already past the current
    /// cursor, then — if nothing was there — block until the journal
    /// signals new data. Once at least one entry is in hand, coalesce
    /// more entries within `batch_wait` up to `batch_size` entries or
    /// `batch_max_bytes` approximate bytes, whichever comes first.
    async fn build_batch(&mut self) -> Result<Batch> {
        if !self.positioned {
            self.position_to_start()?;
        }

        let mut batch = Batch::default();

        // Immediate catch-up. On restart with a saved cursor there is
        // typically a backlog of entries between the last acked cursor and
        // the journal's current tail; reading them before awaiting fd
        // events lets us drain the backlog right away instead of stalling
        // for up to batch_wait (or forever, on a quiet system).
        self.drain_available(&mut batch)?;

        // Block until at least one entry is available if the catch-up
        // found nothing.
        while batch.entries.is_empty() {
            self.wait_for_journal_event().await?;
            self.drain_available(&mut batch)?;
        }

        // Coalesce more entries within the batch window.
        let deadline = Instant::now() + self.batch_wait;
        while self.batch_has_room(&batch) {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let wakeup = tokio::select! {
                r = self.wait_for_journal_event() => r,
                _ = tokio::time::sleep(remaining) => break,
            };
            wakeup?;
            self.drain_available(&mut batch)?;
        }

        tracing::trace!(
            entries = batch.entries.len(),
            approx_bytes = batch.approx_bytes,
            "journald: built batch"
        );
        Ok(batch)
    }

    fn persist_cursor(&self, cursor: &str) -> Result<()> {
        if let Some(parent) = self.cursor_file.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| Error::FileRead {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }
        let tmp = self.cursor_file.with_extension("tmp");
        std::fs::write(&tmp, cursor).map_err(|e| Error::FileRead {
            path: tmp.clone(),
            source: e,
        })?;
        std::fs::rename(&tmp, &self.cursor_file).map_err(|e| Error::FileRead {
            path: self.cursor_file.clone(),
            source: e,
        })?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl Source for JournaldSource {
    async fn peek_batch(&mut self) -> Result<Option<Arc<Batch>>> {
        // Retry path: previous send failed, caller asks again. Top up the
        // in-flight batch with any entries that accumulated during backoff,
        // non-blocking.
        if let Some(arc) = self.in_flight.take() {
            let (mut batch, from_clone) = match Arc::try_unwrap(arc) {
                Ok(b) => (b, false),
                Err(shared) => {
                    // Caller still holds a reference — shouldn't happen in
                    // practice because the shipper drops its Arc between
                    // iterations, but fall back to returning the same Arc
                    // without mutation.
                    self.in_flight = Some(shared.clone());
                    return Ok(Some(shared));
                }
            };
            // Process any pending inotify events without blocking, then
            // drain whatever's available.
            if let Err(e) = self.journal.process() {
                tracing::warn!(error = %e, "journald: process() during top-up failed");
            }
            let before = batch.entries.len();
            self.drain_available(&mut batch)?;
            if batch.entries.len() > before {
                tracing::debug!(
                    added = batch.entries.len() - before,
                    total = batch.entries.len(),
                    approx_bytes = batch.approx_bytes,
                    "journald: topped up in-flight batch"
                );
            }
            let _ = from_clone;
            let arc = Arc::new(batch);
            self.in_flight = Some(arc.clone());
            return Ok(Some(arc));
        }

        // Happy path: build a fresh batch.
        let batch = self.build_batch().await?;
        let arc = Arc::new(batch);
        self.in_flight = Some(arc.clone());
        Ok(Some(arc))
    }

    async fn ack(&mut self, cursor: &Cursor) -> Result<()> {
        self.persist_cursor(cursor)?;
        self.last_acked = Some(cursor.clone());
        self.in_flight = None;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn read_saved_cursor(path: &PathBuf) -> Result<Option<Cursor>> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::FileRead {
            path: path.clone(),
            source: e,
        }),
    }
}

