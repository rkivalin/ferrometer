use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub instance: InstanceConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub logs: LogsConfig,
}

#[derive(Debug, Deserialize)]
pub struct InstanceConfig {
    pub name: String,
}

/// Shared TLS configuration. Flattened into every component that talks to
/// an HTTP backend. Orthogonal to AuthConfig: mTLS provides transport
/// identity, AuthConfig provides per-request credentials; either can be
/// used alone or together.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Fields read by tls::configure at component build time.
pub struct TlsConfig {
    /// PEM-encoded client certificate (chain) for mTLS. May also contain
    /// the private key, in which case `client-key-file` can be omitted.
    pub client_cert_file: Option<PathBuf>,
    /// PEM-encoded private key. Optional; defaults to looking inside
    /// `client-cert-file`.
    pub client_key_file: Option<PathBuf>,
    /// PEM-encoded CA bundle to add to the trust store. Useful for
    /// trusting a private CA that signed the server's certificate.
    pub ca_cert_file: Option<PathBuf>,
}

impl TlsConfig {
    pub fn validate(&self, ctx: &str) -> Result<()> {
        if self.client_key_file.is_some() && self.client_cert_file.is_none() {
            return Err(Error::Config(format!(
                "{ctx}: client-key-file is set but client-cert-file is not"
            )));
        }
        Ok(())
    }

    fn resolve_placeholders(&mut self, instance_name: &str) -> Result<()> {
        if let Some(p) = self.client_cert_file.take() {
            self.client_cert_file = Some(expand_path(&p, instance_name)?);
        }
        if let Some(p) = self.client_key_file.take() {
            self.client_key_file = Some(expand_path(&p, instance_name)?);
        }
        if let Some(p) = self.ca_cert_file.take() {
            self.ca_cert_file = Some(expand_path(&p, instance_name)?);
        }
        Ok(())
    }
}

/// Shared auth configuration. Flattened into every component that talks to
/// an HTTP backend (OTLP forwarder, Loki sink, Prometheus scraper). Exactly
/// one of {basic, bearer, authorization} may be set.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Fields read by auth::resolve_header at component build time.
pub struct AuthConfig {
    pub username: Option<String>,
    pub password: Option<String>,
    pub password_file: Option<PathBuf>,
    pub bearer_token: Option<String>,
    pub bearer_token_file: Option<PathBuf>,
    /// Full Authorization header value. Escape hatch for digest, custom
    /// schemes, etc. Composes with `${env:VAR}` for things like
    /// `authorization = "Bearer ${env:OTLP_TOKEN}"`.
    pub authorization: Option<String>,
}

impl AuthConfig {
    /// Structural validation. Run at config load.
    pub fn validate(&self, ctx: &str) -> Result<()> {
        let basic =
            self.username.is_some() || self.password.is_some() || self.password_file.is_some();
        let bearer = self.bearer_token.is_some() || self.bearer_token_file.is_some();
        let header = self.authorization.is_some();

        let methods = (basic as u8) + (bearer as u8) + (header as u8);
        if methods > 1 {
            return Err(Error::Config(format!(
                "{ctx}: multiple auth methods configured; pick one of \
                 username/password[-file], bearer-token[-file], or authorization"
            )));
        }

        if self.password.is_some() && self.password_file.is_some() {
            return Err(Error::Config(format!(
                "{ctx}: password and password-file are mutually exclusive"
            )));
        }
        if self.bearer_token.is_some() && self.bearer_token_file.is_some() {
            return Err(Error::Config(format!(
                "{ctx}: bearer-token and bearer-token-file are mutually exclusive"
            )));
        }
        if basic
            && self.username.is_some()
            && self.password.is_none()
            && self.password_file.is_none()
        {
            return Err(Error::Config(format!(
                "{ctx}: username set but no password / password-file"
            )));
        }
        if basic && self.username.is_none() {
            return Err(Error::Config(format!(
                "{ctx}: password / password-file set but no username"
            )));
        }
        Ok(())
    }

    /// Resolve `${env:VAR}` etc. in every string-bearing field.
    fn resolve_placeholders(&mut self, instance_name: &str) -> Result<()> {
        if let Some(s) = self.username.take() {
            self.username = Some(expand_placeholders(&s, instance_name)?);
        }
        if let Some(s) = self.password.take() {
            self.password = Some(expand_placeholders(&s, instance_name)?);
        }
        if let Some(p) = self.password_file.take() {
            self.password_file = Some(expand_path(&p, instance_name)?);
        }
        if let Some(s) = self.bearer_token.take() {
            self.bearer_token = Some(expand_placeholders(&s, instance_name)?);
        }
        if let Some(p) = self.bearer_token_file.take() {
            self.bearer_token_file = Some(expand_path(&p, instance_name)?);
        }
        if let Some(s) = self.authorization.take() {
            self.authorization = Some(expand_placeholders(&s, instance_name)?);
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct MetricsConfig {
    #[serde(default)]
    pub collectors: HashMap<String, CollectorConfig>,
    #[serde(default)]
    pub forwarders: HashMap<String, ForwarderConfig>,
}

// Logs: shippers map, each entry pairs one source with one sink.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LogsConfig {
    #[serde(default)]
    pub shippers: HashMap<String, ShipperConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Fields consumed only when log features are compiled.
pub struct ShipperConfig {
    pub source: LogSourceConfig,
    pub sink: LogSinkConfig,
    /// Exponential backoff for failed ships. Doubles on each consecutive
    /// failure up to `backoff-max`; resets on success. Owned here (not on
    /// the sink) because the retry loop lives in the shipper and is
    /// sink-agnostic.
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_shipper_backoff_initial"
    )]
    pub backoff_initial: Duration,
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_shipper_backoff_max"
    )]
    pub backoff_max: Duration,
}

fn default_shipper_backoff_initial() -> Duration {
    Duration::from_secs(1)
}

fn default_shipper_backoff_max() -> Duration {
    Duration::from_secs(300)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(dead_code)] // Inner payloads consumed only when the matching feature is compiled.
pub enum LogSourceConfig {
    Journald(JournaldSourceConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Fields are read only with log-source-journald-systemd feature.
pub struct JournaldSourceConfig {
    #[serde(default = "default_journald_batch_size")]
    pub batch_size: usize,
    /// Approximate upper bound on the encoded size of one batch. A batch is
    /// closed as soon as adding the next entry would exceed this, regardless
    /// of `batch_size`, so that a backlog of large entries can never grow
    /// into a request the sink rejects outright (Loki's gRPC limit is 4 MiB).
    /// Accepts a plain byte count or a string with a K/M/G suffix (SI,
    /// powers of 1000; `KiB`/`MiB`/`GiB` are binary). Set to 0 to disable.
    #[serde(
        deserialize_with = "deserialize_byte_size",
        default = "default_journald_batch_max_bytes"
    )]
    pub batch_max_bytes: usize,
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_journald_batch_wait"
    )]
    pub batch_wait: Duration,
    #[serde(default = "default_journald_cursor_file")]
    pub cursor_file: PathBuf,
    #[serde(default)]
    pub runtime_only: bool,
    /// label-name → journal-field. The journal-field's value becomes the
    /// label's value on each entry; entries missing the field get no such
    /// label. Produces stream labels (low cardinality).
    #[serde(default = "default_journald_labels")]
    pub labels: std::collections::BTreeMap<String, String>,
    /// Static labels added to every entry regardless of source fields.
    #[serde(default)]
    pub static_labels: std::collections::BTreeMap<String, String>,
    /// metadata-name → journal-field. Per-entry, not stream-generating.
    #[serde(default = "default_journald_metadata")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(dead_code)] // Inner payloads consumed only when the matching feature is compiled.
pub enum LogSinkConfig {
    Loki(LokiSinkConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Fields are read only when log-sink-loki feature is enabled.
pub struct LokiSinkConfig {
    pub endpoint: String,
    #[serde(flatten)]
    pub auth: AuthConfig,
    #[serde(flatten)]
    pub tls: TlsConfig,
}

fn default_journald_batch_size() -> usize {
    1000
}

fn default_journald_batch_max_bytes() -> usize {
    // ~Promtail's default batch size; comfortably under Loki's 4 MiB gRPC
    // message limit even after per-stream/protobuf overhead.
    1_000_000
}

fn default_journald_batch_wait() -> Duration {
    Duration::from_secs(5)
}

fn default_journald_cursor_file() -> PathBuf {
    // Resolved at load time via expand_placeholders. Under the shipped
    // systemd unit, StateDirectory=ferrometer causes systemd to set
    // STATE_DIRECTORY=/var/lib/ferrometer for the service.
    PathBuf::from("${env:STATE_DIRECTORY}/journal.cursor")
}

fn default_journald_labels() -> std::collections::BTreeMap<String, String> {
    [("unit", "_SYSTEMD_UNIT"), ("priority", "PRIORITY")]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn default_journald_metadata() -> std::collections::BTreeMap<String, String> {
    [
        ("pid", "_PID"),
        ("syslog_identifier", "SYSLOG_IDENTIFIER"),
        ("syslog_facility", "SYSLOG_FACILITY"),
        ("transport", "_TRANSPORT"),
        ("hostname", "_HOSTNAME"),
        ("boot_id", "_BOOT_ID"),
        ("machine_id", "_MACHINE_ID"),
        ("cmdline", "_CMDLINE"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(dead_code)] // Variants consumed only when the matching feature is compiled.
pub enum CollectorConfig {
    Unix(UnixCollectorConfig),
    Prometheus(PrometheusCollectorConfig),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Fields read only with collector-prometheus feature.
pub struct PrometheusCollectorConfig {
    pub url: String,
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_interval"
    )]
    pub interval: Duration,
    /// HTTP request timeout used by the scraper's reqwest client. The
    /// scheduler's hard deadline (`max_runtime`) should be >= this.
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_prometheus_scrape_timeout"
    )]
    pub scrape_timeout: Duration,
    /// Scheduler-level hard deadline. Defaults generously above
    /// `scrape_timeout` to leave room for connection setup, body read,
    /// and parsing.
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_prometheus_max_runtime"
    )]
    pub max_runtime: Duration,
    #[serde(flatten)]
    pub auth: AuthConfig,
    #[serde(flatten)]
    pub tls: TlsConfig,
    /// Static labels added to every scraped metric. `instance` is
    /// auto-injected from [instance.name] and does not need to be listed
    /// here (though an explicit entry here would win).
    #[serde(default)]
    pub static_labels: std::collections::BTreeMap<String, String>,
}

fn default_prometheus_scrape_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_prometheus_max_runtime() -> Duration {
    Duration::from_secs(15)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct UnixCollectorConfig {
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_interval"
    )]
    pub interval: Duration,
    /// Hard deadline: if the collect call exceeds this, the scheduler aborts
    /// the task. `/proc` reads are effectively instant; the default leaves
    /// generous headroom for statvfs on stale network mounts.
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_unix_max_runtime"
    )]
    pub max_runtime: Duration,
    #[serde(default = "default_disk_devices")]
    pub disk_devices: String,
    #[serde(default = "default_net_devices")]
    pub net_devices: String,
    /// Include-regex over mountpoints. Empty (the default) matches all;
    /// applied on top of the hardcoded pseudo-fs floor.
    #[serde(default)]
    pub filesystem_mount_points: String,
    /// Include-regex over filesystem types. Empty (the default) matches all;
    /// applied on top of the hardcoded pseudo-fs floor.
    #[serde(default)]
    pub filesystem_fs_types: String,
    /// Collapse bind mounts / multiple mountpoints sharing one block device
    /// down to a single canonical entry. Default true.
    #[serde(default = "default_true")]
    pub filesystem_dedupe_devices: bool,
    #[serde(default = "default_unix_collectors")]
    pub collectors: Vec<String>,
    /// Static labels added to every emitted metric. Nothing is auto-injected;
    /// to carry an `instance`-like identifier prefer the forwarder's
    /// resource-attributes config (OTel semantic convention).
    #[serde(default)]
    pub static_labels: std::collections::BTreeMap<String, String>,
}

fn default_unix_max_runtime() -> Duration {
    Duration::from_secs(5)
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ForwarderConfig {
    Otlphttp(OtlphttpForwarderConfig),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct OtlphttpForwarderConfig {
    pub endpoint: String,
    #[serde(flatten)]
    pub auth: AuthConfig,
    #[serde(flatten)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub compression: Compression,
    #[serde(deserialize_with = "deserialize_duration", default = "default_timeout")]
    pub timeout: Duration,
    #[serde(default = "default_buffer_max_metrics")]
    pub buffer_max_metrics: usize,
    #[serde(default = "default_request_max_metrics")]
    pub request_max_metrics: usize,
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_backoff_initial"
    )]
    pub backoff_initial: Duration,
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_backoff_max"
    )]
    pub backoff_max: Duration,
    /// Resource attributes attached to the OTLP Resource message (as opposed
    /// to per-datapoint attributes, which are the metric labels themselves).
    /// Values may contain placeholders (`${instance.name}`, `${version}`,
    /// `${env:VAR}`) resolved at config load time.
    #[serde(default)]
    pub resource_attributes: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    Gzip,
    None,
}

// Defaults

fn default_interval() -> Duration {
    Duration::from_secs(15)
}

fn default_disk_devices() -> String {
    "^(nvme\\d+n\\d+|sd[a-z]+|vd[a-z]+)$".into()
}

fn default_net_devices() -> String {
    "^(eth|en|wl|bond)".into()
}

fn default_unix_collectors() -> Vec<String> {
    [
        "cpu",
        "memory",
        "disk",
        "filesystem",
        "md",
        "netdev",
        "loadavg",
        "uname",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect()
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_buffer_max_metrics() -> usize {
    100_000
}

fn default_request_max_metrics() -> usize {
    10_000
}

fn default_backoff_initial() -> Duration {
    Duration::from_secs(1)
}

fn default_backoff_max() -> Duration {
    Duration::from_secs(300)
}

// Duration parsing

fn deserialize_duration<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    if let Some(n) = s.strip_suffix('s') {
        Ok(Duration::from_secs(
            n.parse::<u64>().map_err(|e| e.to_string())?,
        ))
    } else if let Some(n) = s.strip_suffix('m') {
        Ok(Duration::from_secs(
            n.parse::<u64>().map_err(|e| e.to_string())? * 60,
        ))
    } else if let Some(n) = s.strip_suffix('h') {
        Ok(Duration::from_secs(
            n.parse::<u64>().map_err(|e| e.to_string())? * 3600,
        ))
    } else {
        Err(format!(
            "invalid duration '{s}': expected suffix s, m, or h"
        ))
    }
}

// Byte-size parsing

fn deserialize_byte_size<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Int(u64),
        Str(String),
    }
    match Raw::deserialize(deserializer)? {
        Raw::Int(n) => usize::try_from(n).map_err(serde::de::Error::custom),
        Raw::Str(s) => parse_byte_size(&s).map_err(serde::de::Error::custom),
    }
}

/// Parse a byte count: a bare integer, or an integer followed by a unit
/// (case-insensitive, whitespace allowed). `K`/`KB`, `M`/`MB`, `G`/`GB` are
/// SI (powers of 1000); `KiB`/`MiB`/`GiB` are binary (powers of 1024).
fn parse_byte_size(s: &str) -> std::result::Result<usize, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid byte size '{s}': expected <integer>[K|M|G]"))?;
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1_000,
        "m" | "mb" => 1_000_000,
        "g" | "gb" => 1_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        other => {
            return Err(format!(
                "invalid byte size '{s}': unknown unit '{other}' (expected K, M, or G)"
            ));
        }
    };
    n.checked_mul(mult)
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(|| format!("invalid byte size '{s}': out of range"))
}

// Config loading and validation

impl Config {
    /// Load + shape-validate + resolve placeholders. Used by `run`.
    pub fn load(path: &Path) -> Result<Self> {
        let mut config = Self::load_unresolved(path)?;
        config.resolve_placeholders()?;
        Ok(config)
    }

    /// Load + shape-validate only, without resolving placeholders. Used by
    /// `validate`: an admin running `ferrometer validate` from a shell does
    /// not have the service's `STATE_DIRECTORY` / `CREDENTIALS_DIRECTORY`
    /// env vars in scope, but the config is still well-formed.
    pub fn load_unresolved(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read {}: {e}", path.display())))?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| Error::Config(format!("failed to parse config: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Walk every placeholder-bearing field and substitute. The set of fields
    /// is listed here explicitly rather than derived reflectively so the
    /// contract is obvious at the call site.
    fn resolve_placeholders(&mut self) -> Result<()> {
        let instance_name = self.instance.name.clone();

        for collector in self.metrics.collectors.values_mut() {
            match collector {
                CollectorConfig::Unix(_) => {}
                CollectorConfig::Prometheus(c) => {
                    c.auth.resolve_placeholders(&instance_name)?;
                    c.tls.resolve_placeholders(&instance_name)?;
                }
            }
        }

        for forwarder in self.metrics.forwarders.values_mut() {
            match forwarder {
                ForwarderConfig::Otlphttp(c) => {
                    c.auth.resolve_placeholders(&instance_name)?;
                    c.tls.resolve_placeholders(&instance_name)?;
                    for v in c.resource_attributes.values_mut() {
                        *v = expand_placeholders(v, &instance_name)?;
                    }
                }
            }
        }

        for shipper in self.logs.shippers.values_mut() {
            match &mut shipper.source {
                LogSourceConfig::Journald(c) => {
                    c.cursor_file = expand_path(&c.cursor_file, &instance_name)?;
                }
            }
            match &mut shipper.sink {
                LogSinkConfig::Loki(c) => {
                    c.auth.resolve_placeholders(&instance_name)?;
                    c.tls.resolve_placeholders(&instance_name)?;
                }
            }
        }

        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.instance.name.is_empty() {
            return Err(Error::Config("instance.name must not be empty".into()));
        }
        if self.metrics.collectors.is_empty() {
            return Err(Error::Config(
                "at least one collector must be configured".into(),
            ));
        }
        if self.metrics.forwarders.is_empty() {
            return Err(Error::Config(
                "at least one forwarder must be configured".into(),
            ));
        }

        for (name, collector) in &self.metrics.collectors {
            match collector {
                CollectorConfig::Unix(unix) => {
                    self.validate_unix_collector(name, unix)?;
                }
                CollectorConfig::Prometheus(prom) => {
                    self.validate_prometheus_collector(name, prom)?;
                }
            }
        }

        for (name, forwarder) in &self.metrics.forwarders {
            match forwarder {
                ForwarderConfig::Otlphttp(otlp) => {
                    self.validate_otlphttp_forwarder(name, otlp)?;
                }
            }
        }

        for (name, shipper) in &self.logs.shippers {
            if shipper.backoff_initial > shipper.backoff_max {
                return Err(Error::Config(format!(
                    "shipper {name}: backoff-initial must not exceed backoff-max"
                )));
            }
            match &shipper.sink {
                LogSinkConfig::Loki(c) => {
                    c.auth.validate(&format!("shipper {name} sink"))?;
                    c.tls.validate(&format!("shipper {name} sink"))?;
                }
            }
        }

        Ok(())
    }

    fn validate_unix_collector(&self, name: &str, config: &UnixCollectorConfig) -> Result<()> {
        #[cfg(feature = "collector-unix")]
        {
            regex::Regex::new(&config.disk_devices).map_err(|e| {
                Error::Config(format!("collector {name}: invalid disk-devices regex: {e}"))
            })?;
            regex::Regex::new(&config.net_devices).map_err(|e| {
                Error::Config(format!("collector {name}: invalid net-devices regex: {e}"))
            })?;
            regex::Regex::new(&config.filesystem_mount_points).map_err(|e| {
                Error::Config(format!(
                    "collector {name}: invalid filesystem-mount-points regex: {e}"
                ))
            })?;
            regex::Regex::new(&config.filesystem_fs_types).map_err(|e| {
                Error::Config(format!(
                    "collector {name}: invalid filesystem-fs-types regex: {e}"
                ))
            })?;
        }
        #[cfg(not(feature = "collector-unix"))]
        let _ = config;

        let valid = [
            "cpu",
            "memory",
            "disk",
            "filesystem",
            "md",
            "netdev",
            "loadavg",
            "uname",
        ];
        for c in &config.collectors {
            if !valid.contains(&c.as_str()) {
                return Err(Error::Config(format!(
                    "collector {name}: unknown sub-collector '{c}'"
                )));
            }
        }
        Ok(())
    }

    fn validate_prometheus_collector(
        &self,
        name: &str,
        config: &PrometheusCollectorConfig,
    ) -> Result<()> {
        if config.url.is_empty() {
            return Err(Error::Config(format!(
                "collector {name}: url must not be empty"
            )));
        }
        config.auth.validate(&format!("collector {name}"))?;
        config.tls.validate(&format!("collector {name}"))?;
        Ok(())
    }

    fn validate_otlphttp_forwarder(
        &self,
        name: &str,
        config: &OtlphttpForwarderConfig,
    ) -> Result<()> {
        if config.endpoint.is_empty() {
            return Err(Error::Config(format!(
                "forwarder {name}: endpoint must not be empty"
            )));
        }
        config.auth.validate(&format!("forwarder {name}"))?;
        config.tls.validate(&format!("forwarder {name}"))?;
        if config.buffer_max_metrics == 0 {
            return Err(Error::Config(format!(
                "forwarder {name}: buffer-max-metrics must be greater than 0"
            )));
        }
        if config.request_max_metrics == 0 {
            return Err(Error::Config(format!(
                "forwarder {name}: request-max-metrics must be greater than 0"
            )));
        }
        if config.backoff_initial > config.backoff_max {
            return Err(Error::Config(format!(
                "forwarder {name}: backoff-initial must not exceed backoff-max"
            )));
        }
        Ok(())
    }
}

// Placeholder expansion
//
// Supported forms inside `${...}`:
//   env:VAR          — value of environment variable VAR
//   instance.name    — the top-level [instance].name
//   version          — ferrometer's own version (CARGO_PKG_VERSION)
//
// Unknown placeholders and unset env vars are hard errors at load time.

pub(crate) fn expand_placeholders(s: &str, instance_name: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err(Error::Config(format!(
                "unterminated placeholder in config value '{s}'"
            )));
        };
        let inner = &after[..end];
        let value = resolve_placeholder(inner, instance_name)
            .map_err(|e| Error::Config(format!("in config value '{s}': {e}")))?;
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn resolve_placeholder(inner: &str, instance_name: &str) -> std::result::Result<String, String> {
    if inner == "instance.name" {
        Ok(instance_name.to_string())
    } else if inner == "version" {
        Ok(env!("CARGO_PKG_VERSION").to_string())
    } else if let Some(name) = inner.strip_prefix("env:") {
        std::env::var(name).map_err(|_| {
            format!("${{env:{name}}} referenced but environment variable '{name}' is not set")
        })
    } else {
        Err(format!("unknown placeholder '${{{inner}}}'"))
    }
}

fn expand_path(p: &Path, instance_name: &str) -> Result<PathBuf> {
    let s = p
        .to_str()
        .ok_or_else(|| Error::Config(format!("non-utf8 path in config: {}", p.display())))?;
    Ok(PathBuf::from(expand_placeholders(s, instance_name)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_size_parsing() {
        assert_eq!(parse_byte_size("0"), Ok(0));
        assert_eq!(parse_byte_size("1048576"), Ok(1 << 20));
        assert_eq!(parse_byte_size("1M"), Ok(1_000_000));
        assert_eq!(parse_byte_size("2 mb"), Ok(2_000_000));
        assert_eq!(parse_byte_size("512K"), Ok(512_000));
        assert_eq!(parse_byte_size("1g"), Ok(1_000_000_000));
        assert_eq!(parse_byte_size("1MiB"), Ok(1 << 20));
        assert_eq!(parse_byte_size("1KiB"), Ok(1024));
        assert_eq!(parse_byte_size("1 gib"), Ok(1 << 30));
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("M").is_err());
        assert!(parse_byte_size("1T").is_err());
        assert!(parse_byte_size("1.5M").is_err());
    }
}
