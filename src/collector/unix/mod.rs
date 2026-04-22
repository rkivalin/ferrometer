use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;

use crate::collector::Collector;
use crate::config::UnixCollectorConfig;
use crate::error::Result;
use crate::signal::{Labels, Metric};

mod cpu;
mod disk;
mod filesystem;
mod loadavg;
mod memory;
mod netdev;
mod uname;

/// Intern cache for label sets. Sub-collectors build a raw `Labels` each tick;
/// `intern` folds in the instance label, deduplicates against previously seen
/// sets, and returns a shared `Arc<Labels>`. Since the label shape is stable
/// across ticks for this collector, the cache grows once and is reused
/// indefinitely.
pub(crate) struct LabelCache {
    instance: String,
    cache: HashSet<Arc<Labels>>,
}

impl LabelCache {
    fn new(instance: String) -> Self {
        Self {
            instance,
            cache: HashSet::new(),
        }
    }

    pub(crate) fn intern(&mut self, mut labels: Labels) -> Arc<Labels> {
        labels.insert("instance".to_string(), self.instance.clone());
        if let Some(arc) = self.cache.get(&labels) {
            return arc.clone();
        }
        let arc = Arc::new(labels);
        self.cache.insert(arc.clone());
        arc
    }
}

pub struct UnixCollector {
    #[allow(dead_code)]
    name: String,
    enabled: Vec<String>,
    disk_filter: Regex,
    net_filter: Regex,
    cache: LabelCache,
}

impl UnixCollector {
    pub fn new(name: &str, config: &UnixCollectorConfig, instance: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            enabled: config.collectors.clone(),
            disk_filter: Regex::new(&config.disk_devices).map_err(|e| {
                crate::error::Error::Config(format!("invalid disk-devices regex: {e}"))
            })?,
            net_filter: Regex::new(&config.net_devices).map_err(|e| {
                crate::error::Error::Config(format!("invalid net-devices regex: {e}"))
            })?,
            cache: LabelCache::new(instance.to_string()),
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
                "filesystem" => filesystem::collect(&mut self.cache)?,
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
