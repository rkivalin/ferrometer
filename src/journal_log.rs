//! `tracing-subscriber` event formatter that emits lines in the syslog-style
//! `<N>` priority-prefix form journald parses out of stderr (see `sd-daemon(3)`,
//! "Logging" section). Used when `JOURNAL_STREAM` is set, i.e. when our stderr
//! is hooked up to the journal.
//!
//! Without this, every line would land in the journal at PRIORITY=info and
//! carry a duplicated timestamp ahead of journald's own. With it, each entry
//! gets the right priority and journalctl displays it cleanly.

use std::fmt;

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::{
    FmtContext, FormatEvent, FormatFields, format::Writer,
};
use tracing_subscriber::registry::LookupSpan;

pub struct JournalFormat;

impl<S, N> FormatEvent<S, N> for JournalFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // Map tracing levels to syslog priority numbers per RFC 5424.
        let priority = match *event.metadata().level() {
            Level::ERROR => 3, // err
            Level::WARN => 4,  // warning
            Level::INFO => 6,  // info
            Level::DEBUG | Level::TRACE => 7, // debug
        };
        write!(writer, "<{priority}>")?;
        write!(writer, "{}: ", event.metadata().target())?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// True when systemd has hooked our stderr to the journal. The variable
/// holds `<dev>:<inode>` of the stream and is documented in systemd.exec(5).
pub fn under_journald() -> bool {
    std::env::var_os("JOURNAL_STREAM").is_some()
}
