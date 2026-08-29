use async_trait::async_trait;
use regex::Regex;

use crate::collector::Collector;
use crate::collector::label_cache::LabelCache;
use crate::config::UnixCollectorConfig;
use crate::error::Result;
use crate::signal::{Labels, Metric};

mod cpu;
mod disk;
mod filesystem;
mod hwmon;
mod loadavg;
mod md;
mod memory;
mod netdev;
mod uname;

pub struct UnixCollector {
    #[allow(dead_code)]
    name: String,
    enabled: Vec<String>,
    disk_filter: Regex,
    net_filter: Regex,
    fs_mount_filter: Regex,
    fs_type_filter: Regex,
    fs_dedupe_devices: bool,
    hwmon_filter: Regex,
    cache: LabelCache,
}

impl UnixCollector {
    pub fn new(name: &str, config: &UnixCollectorConfig) -> Result<Self> {
        let base: Labels = config
            .static_labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(Self {
            name: name.to_string(),
            enabled: config.collectors.clone(),
            disk_filter: Regex::new(&config.disk_devices).map_err(|e| {
                crate::error::Error::Config(format!("invalid disk-devices regex: {e}"))
            })?,
            net_filter: Regex::new(&config.net_devices).map_err(|e| {
                crate::error::Error::Config(format!("invalid net-devices regex: {e}"))
            })?,
            fs_mount_filter: Regex::new(&config.filesystem_mount_points).map_err(|e| {
                crate::error::Error::Config(format!("invalid filesystem-mount-points regex: {e}"))
            })?,
            fs_type_filter: Regex::new(&config.filesystem_fs_types).map_err(|e| {
                crate::error::Error::Config(format!("invalid filesystem-fs-types regex: {e}"))
            })?,
            fs_dedupe_devices: config.filesystem_dedupe_devices,
            hwmon_filter: Regex::new(&config.hwmon_chips).map_err(|e| {
                crate::error::Error::Config(format!("invalid hwmon-chips regex: {e}"))
            })?,
            cache: LabelCache::new(base),
        })
    }
}

#[async_trait]
impl Collector for UnixCollector {
    async fn collect(&mut self) -> Result<Vec<Metric>> {
        let mut all = Vec::new();
        for sub in &self.enabled {
            let metrics = match sub.as_str() {
                "cpu" => cpu::collect(&mut self.cache)?,
                "memory" => memory::collect(&mut self.cache)?,
                "disk" => disk::collect(&mut self.cache, &self.disk_filter)?,
                "filesystem" => {
                    filesystem::collect(
                        &mut self.cache,
                        &self.fs_mount_filter,
                        &self.fs_type_filter,
                        self.fs_dedupe_devices,
                    )
                    .await?
                }
                "md" => md::collect(&mut self.cache)?,
                "hwmon" => hwmon::collect(&mut self.cache, &self.hwmon_filter)?,
                "netdev" => netdev::collect(&mut self.cache, &self.net_filter)?,
                "loadavg" => loadavg::collect(&mut self.cache)?,
                "uname" => uname::collect(&mut self.cache)?,
                _ => continue,
            };
            all.extend(metrics);
        }
        Ok(all)
    }

    fn name(&self) -> &str {
        &self.name
    }
}
