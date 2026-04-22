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
}

#[derive(Debug, Deserialize)]
pub struct InstanceConfig {
    pub name: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct MetricsConfig {
    #[serde(default)]
    pub collectors: HashMap<String, CollectorConfig>,
    #[serde(default)]
    pub forwarders: HashMap<String, ForwarderConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CollectorConfig {
    Unix(UnixCollectorConfig),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct UnixCollectorConfig {
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_interval"
    )]
    pub interval: Duration,
    #[serde(default = "default_disk_devices")]
    pub disk_devices: String,
    #[serde(default = "default_net_devices")]
    pub net_devices: String,
    #[serde(default = "default_unix_collectors")]
    pub collectors: Vec<String>,
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
    pub username: Option<String>,
    pub password_file: Option<PathBuf>,
    #[serde(default)]
    pub compression: Compression,
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_timeout"
    )]
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
    ["cpu", "memory", "disk", "filesystem", "netdev", "loadavg", "uname"]
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

// Config loading and validation

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            Error::Config(format!("failed to read {}: {e}", path.display()))
        })?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| Error::Config(format!("failed to parse config: {e}")))?;
        config.validate()?;
        Ok(config)
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
            }
        }

        for (name, forwarder) in &self.metrics.forwarders {
            match forwarder {
                ForwarderConfig::Otlphttp(otlp) => {
                    self.validate_otlphttp_forwarder(name, otlp)?;
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
        }
        #[cfg(not(feature = "collector-unix"))]
        let _ = config;

        let valid = [
            "cpu",
            "memory",
            "disk",
            "filesystem",
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
        if config.username.is_some() && config.password_file.is_none() {
            return Err(Error::Config(format!(
                "forwarder {name}: password-file is required when username is set"
            )));
        }
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
