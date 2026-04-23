use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use systemd::journal::{Journal, JournalRecord, OpenOptions};

use crate::error::{Error, Result};
use crate::log::source::{Batch, Cursor, LogEntry, Source};

/// Hardcoded cursor persistence path for the prototype. Moved to config in
/// later iterations.
const CURSOR_FILE: &str = "./ferrometer-journal.cursor";

pub struct JournaldSource {
    name: String,
    journal: Journal,
    cursor_file: PathBuf,
    last_acked: Option<Cursor>,
    cached: Option<Arc<Batch>>,
    /// Whether the journal has been positioned since the last ack. Before
    /// the first peek, `false` — we must seek either to the saved cursor or
    /// to tail. After each ack, reset to `false` so the next peek re-seeks
    /// past the newly-acked cursor.
    positioned: bool,
}

impl JournaldSource {
    pub fn new(name: &str) -> Result<Self> {
        // runtime_only(true) opens only the volatile journal under
        // /run/log/journal, skipping the persistent archive under
        // /var/log/journal. That dramatically shrinks the mmap surface
        // (often 100s of MB → a few MB) at the cost of not seeing entries
        // that pre-date ferrometer startup. For a tail-follow shipper this
        // is the right trade-off; revisit if we ever need backfill.
        let journal = OpenOptions::default()
            .system(true)
            .local_only(true)
            .runtime_only(false)
            .open()
            .map_err(|e| Error::Source(format!("open journal: {e}")))?;

        let cursor_file = PathBuf::from(CURSOR_FILE);
        let last_acked = read_saved_cursor(&cursor_file)?;

        Ok(Self {
            name: name.to_string(),
            journal,
            cursor_file,
            last_acked,
            cached: None,
            positioned: false,
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
                    // seek_cursor positions AT the cursor entry; advance past it
                    // so the next next_entry() returns the following entry.
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
                // seek_tail positions us past the last entry at seek time.
                // From there, next() won't find newly-appended entries
                // because the tail pointer doesn't auto-follow appends.
                // Calling previous() moves to the last existing entry,
                // establishing a concrete cursor from which subsequent
                // next() calls can advance into new appends.
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

    fn read_entries(&mut self, max: usize) -> Result<Option<Batch>> {
        // Journald mmaps journal files but its internal "tail" position is
        // captured at seek time. New appends arrive via inotify and must be
        // processed via wait() before next_entry() can see them. A zero
        // timeout processes any pending notifications and returns
        // immediately.
        let wait_result = self
            .journal
            .wait(Some(Duration::ZERO))
            .map_err(|e| Error::Source(format!("wait: {e}")))?;
        tracing::trace!(?wait_result, "journald: wait(0) result");

        let mut entries = Vec::new();
        let mut last_cursor = None;
        for _ in 0..max {
            match self
                .journal
                .next_entry()
                .map_err(|e| Error::Source(format!("next_entry: {e}")))?
            {
                Some(record) => {
                    let cursor = self
                        .journal
                        .cursor()
                        .map_err(|e| Error::Source(format!("cursor: {e}")))?;
                    entries.push(record_to_entry(record));
                    last_cursor = Some(cursor);
                }
                None => break,
            }
        }
        tracing::trace!(
            read = entries.len(),
            max,
            "journald: read_entries finished"
        );
        match last_cursor {
            Some(c) => Ok(Some(Batch {
                entries,
                end_cursor: c,
            })),
            None => Ok(None),
        }
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
    async fn peek_batch(&mut self, max: usize) -> Result<Option<Arc<Batch>>> {
        if let Some(batch) = &self.cached {
            return Ok(Some(batch.clone()));
        }

        if !self.positioned {
            self.position_to_start()?;
        }

        let batch = self.read_entries(max)?;
        let arc_batch = batch.map(Arc::new);
        self.cached = arc_batch.clone();
        Ok(arc_batch)
    }

    async fn ack(&mut self, cursor: &Cursor) -> Result<()> {
        self.persist_cursor(cursor)?;
        self.last_acked = Some(cursor.clone());
        self.cached = None;
        // Journal is positioned at the end of the batch already; no re-seek
        // needed on the next peek.
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

fn record_to_entry(mut record: JournalRecord) -> LogEntry {
    let message = record.remove("MESSAGE").unwrap_or_default();
    let timestamp = record
        .get("__REALTIME_TIMESTAMP")
        .and_then(|s| s.parse::<u64>().ok())
        .map(|us| UNIX_EPOCH + Duration::from_micros(us))
        .unwrap_or_else(SystemTime::now);
    LogEntry {
        timestamp,
        message,
        fields: record,
    }
}

