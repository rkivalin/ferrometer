use procfs::CurrentSI;

use crate::collector::unix::LabelCache;
use crate::error::Result;
use crate::signal::{Labels, Metric};

pub fn collect(cache: &mut LabelCache) -> Result<Vec<Metric>> {
    let stats = procfs::KernelStats::current()
        .map_err(|e| crate::error::Error::Collector(format!("cpu: {e}")))?;
    let tps = procfs::ticks_per_second() as f64;
    let mut metrics = Vec::new();

    for (i, cpu) in stats.cpu_time.iter().enumerate() {
        let cpu_label = format!("{i}");
        let modes: &[(&str, f64)] = &[
            ("user", cpu.user as f64 / tps),
            ("nice", cpu.nice as f64 / tps),
            ("system", cpu.system as f64 / tps),
            ("idle", cpu.idle as f64 / tps),
            ("iowait", cpu.iowait.unwrap_or(0) as f64 / tps),
            ("irq", cpu.irq.unwrap_or(0) as f64 / tps),
            ("softirq", cpu.softirq.unwrap_or(0) as f64 / tps),
            ("steal", cpu.steal.unwrap_or(0) as f64 / tps),
        ];
        for &(mode, value) in modes {
            let mut labels = Labels::new();
            labels.insert("cpu".into(), cpu_label.clone());
            labels.insert("mode".into(), mode.into());
            metrics.push(Metric::counter(
                "node_cpu_seconds_total",
                value,
                cache.intern(labels),
            ));
        }
    }

    Ok(metrics)
}
