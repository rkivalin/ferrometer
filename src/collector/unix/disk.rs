use regex::Regex;

use crate::collector::label_cache::LabelCache;
use crate::error::Result;
use crate::signal::{Labels, Metric, MetricType};

pub fn collect(cache: &mut LabelCache, filter: &Regex) -> Result<Vec<Metric>> {
    let stats = procfs::diskstats()
        .map_err(|e| crate::error::Error::Collector(format!("disk: {e}")))?;
    let mut metrics = Vec::new();

    for disk in stats {
        if !filter.is_match(&disk.name) {
            continue;
        }

        let pairs: &[(&'static str, f64, MetricType)] = &[
            (
                "node_disk_reads_completed_total",
                disk.reads as f64,
                MetricType::Counter,
            ),
            (
                "node_disk_read_bytes_total",
                (disk.sectors_read * 512) as f64,
                MetricType::Counter,
            ),
            (
                "node_disk_read_time_seconds_total",
                disk.time_reading as f64 / 1000.0,
                MetricType::Counter,
            ),
            (
                "node_disk_writes_completed_total",
                disk.writes as f64,
                MetricType::Counter,
            ),
            (
                "node_disk_written_bytes_total",
                (disk.sectors_written * 512) as f64,
                MetricType::Counter,
            ),
            (
                "node_disk_write_time_seconds_total",
                disk.time_writing as f64 / 1000.0,
                MetricType::Counter,
            ),
            (
                "node_disk_io_now",
                disk.in_progress as f64,
                MetricType::Gauge,
            ),
            (
                "node_disk_io_time_seconds_total",
                disk.time_in_progress as f64 / 1000.0,
                MetricType::Counter,
            ),
        ];

        let mut labels = Labels::new();
        labels.insert("device".into(), disk.name.clone());
        let labels = cache.intern(labels);

        for (name, value, mtype) in pairs {
            let metric = match mtype {
                MetricType::Gauge => Metric::gauge(name, *value, labels.clone()),
                MetricType::Counter => Metric::counter(name, *value, labels.clone()),
            };
            metrics.push(metric);
        }
    }

    Ok(metrics)
}
