use regex::Regex;

use crate::error::Result;
use crate::signal::{Labels, Metric, MetricType};

pub fn collect(filter: &Regex) -> Result<Vec<Metric>> {
    let stats = procfs::diskstats()
        .map_err(|e| crate::error::Error::Collector(format!("disk: {e}")))?;
    let mut metrics = Vec::new();

    for disk in stats {
        if !filter.is_match(&disk.name) {
            continue;
        }

        let pairs: &[(&str, f64, MetricType)] = &[
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

        for (name, value, mtype) in pairs {
            let mut labels = Labels::new();
            labels.insert("device".into(), disk.name.clone());
            metrics.push(Metric {
                name: (*name).to_string(),
                labels,
                value: *value,
                timestamp: std::time::SystemTime::now(),
                metric_type: mtype.clone(),
            });
        }
    }

    Ok(metrics)
}
