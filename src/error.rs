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
