use async_trait::async_trait;
use regex::Regex;

use crate::collector::Collector;
use crate::config::UnixCollectorConfig;
use crate::error::Result;
use crate::signal::Metric;

mod cpu;
mod disk;
mod filesystem;
mod loadavg;
mod memory;
mod netdev;
mod uname;

pub struct UnixCollector {
    #[allow(dead_code)]
    name: String,
    instance: String,
    enabled: Vec<String>,
    disk_filter: Regex,
    net_filter: Regex,
}

impl UnixCollector {
    pub fn new(name: &str, config: &UnixCollectorConfig, instance: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            instance: instance.to_string(),
            enabled: config.collectors.clone(),
            disk_filter: Regex::new(&config.disk_devices).map_err(|e| {
                crate::error::Error::Config(format!("invalid disk-devices regex: {e}"))
            })?,
            net_filter: Regex::new(&config.net_devices).map_err(|e| {
                crate::error::Error::Config(format!("invalid net-devices regex: {e}"))
            })?,
        })
    }

    fn add_instance_label(&self, metrics: &mut [Metric]) {
        for m in metrics.iter_mut() {
            m.labels
                .insert("instance".to_string(), self.instance.clone());
        }
    }
}

#[async_trait]
impl Collector for UnixCollector {
    async fn collect(&mut self) -> Result<Vec<Metric>> {
        let mut all = Vec::new();
        for sub in &self.enabled {
            let mut metrics = match sub.as_str() {
                "cpu" => cpu::collect()?,
                "memory" => memory::collect()?,
                "disk" => disk::collect(&self.disk_filter)?,
                "filesystem" => filesystem::collect()?,
                "netdev" => netdev::collect(&self.net_filter)?,
                "loadavg" => loadavg::collect()?,
                "uname" => uname::collect()?,
                _ => continue,
            };
            self.add_instance_label(&mut metrics);
            all.extend(metrics);
        }
        Ok(all)
    }

    fn name(&self) -> &str {
        &self.name
    }
}
