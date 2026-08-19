use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("config error: {0}")]
    Config(String),

    #[error("collector error: {0}")]
    Collector(String),

    #[error("forwarder error: {0}")]
    Forwarder(String),

    #[cfg(all(feature = "log-source-journald-systemd", feature = "log-sink-loki"))]
    #[error("log source error: {0}")]
    Source(String),

    #[cfg(all(feature = "log-source-journald-systemd", feature = "log-sink-loki"))]
    #[error("log sink error: {0}")]
    Sink(String),

    /// The sink rejected the request because the payload is too large (HTTP
    /// 413, or a 5xx whose body reports a message-size limit). Retrying the
    /// same payload verbatim can never succeed; the shipper splits instead.
    #[cfg(all(feature = "log-source-journald-systemd", feature = "log-sink-loki"))]
    #[error("log sink rejected payload as too large: {0}")]
    SinkPayloadTooLarge(String),

    /// The sink rejected (part of) the request as invalid — HTTP 400. For
    /// Loki this is a per-entry validation failure (timestamp too old/new,
    /// line too long, bad labels, …): the valid entries *were* ingested and
    /// the offending ones dropped server-side, so retrying cannot succeed.
    /// The shipper logs the rejection and acks.
    #[cfg(all(feature = "log-source-journald-systemd", feature = "log-sink-loki"))]
    #[error("log sink rejected payload: {0}")]
    SinkRejected(String),

    #[error("failed to read {path}: {source}")]
    FileRead {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
