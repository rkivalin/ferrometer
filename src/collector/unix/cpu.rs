use std::collections::HashSet;

use procfs::CurrentSI;

use crate::collector::label_cache::LabelCache;
use crate::error::Result;
use crate::signal::{Labels, Metric};

pub fn collect(cache: &mut LabelCache) -> Result<Vec<Metric>> {
    let stats = procfs::KernelStats::current()
        .map_err(|e| crate::error::Error::Collector(format!("cpu: {e}")))?;
    let tps = procfs::ticks_per_second() as f64;
    let mut metrics = Vec::new();

    let mut seen_packages: HashSet<String> = HashSet::new();
    for (i, cpu) in stats.cpu_time.iter().enumerate() {
        let cpu_label = format!("{i}");
        // Per-CPU topology — used as a label on all per-cpu metrics and to
        // gate the per-package throttle emission.
        let package = read_string(&format!(
            "/sys/devices/system/cpu/cpu{i}/topology/physical_package_id"
        ));

        // Per-mode CPU time counters (procfs).
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

        // Frequency metrics from /sys/devices/system/cpu/cpuN/cpufreq/*.
        // These don't exist on VMs / hypervisors that don't expose cpufreq,
        // so we skip silently on absence.
        let cpufreq_dir = format!("/sys/devices/system/cpu/cpu{i}/cpufreq");
        let freq_fields: &[(&str, &'static str)] = &[
            ("scaling_cur_freq", "node_cpu_scaling_frequency_hertz"),
            ("scaling_min_freq", "node_cpu_scaling_frequency_min_hertz"),
            ("scaling_max_freq", "node_cpu_scaling_frequency_max_hertz"),
            ("cpuinfo_min_freq", "node_cpu_frequency_min_hertz"),
            ("cpuinfo_max_freq", "node_cpu_frequency_max_hertz"),
        ];
        for &(file, metric_name) in freq_fields {
            if let Some(khz) = read_u64(&format!("{cpufreq_dir}/{file}")) {
                let mut labels = Labels::new();
                labels.insert("cpu".into(), cpu_label.clone());
                // Values are in kHz; convert to Hz.
                metrics.push(Metric::gauge(
                    metric_name,
                    khz as f64 * 1000.0,
                    cache.intern(labels),
                ));
            }
        }

        // Governor: emitted as a gauge of 1 with the governor name as a
        // label, matching node_exporter's convention.
        if let Some(governor) = read_string(&format!("{cpufreq_dir}/scaling_governor")) {
            let mut labels = Labels::new();
            labels.insert("cpu".into(), cpu_label.clone());
            labels.insert("governor".into(), governor);
            metrics.push(Metric::gauge(
                "node_cpu_scaling_governor",
                1.0,
                cache.intern(labels),
            ));
        }

        // Per-core thermal throttle count (Intel). Carries the package
        // label too so queries can filter per socket.
        let throttle_dir = format!("/sys/devices/system/cpu/cpu{i}/thermal_throttle");
        if let Some(count) = read_u64(&format!("{throttle_dir}/core_throttle_count")) {
            let mut labels = Labels::new();
            labels.insert("cpu".into(), cpu_label.clone());
            if let Some(pkg) = &package {
                labels.insert("package".into(), pkg.clone());
            }
            metrics.push(Metric::counter(
                "node_cpu_core_throttles_total",
                count as f64,
                cache.intern(labels),
            ));
        }

        // Per-package thermal throttle count (Intel). Each CPU in a package
        // reports the same counter via sysfs, so emit once per unique
        // physical_package_id encountered.
        if let Some(pkg) = &package
            && seen_packages.insert(pkg.clone())
            && let Some(count) = read_u64(&format!("{throttle_dir}/package_throttle_count"))
        {
            let mut labels = Labels::new();
            labels.insert("package".into(), pkg.clone());
            metrics.push(Metric::counter(
                "node_cpu_package_throttles_total",
                count as f64,
                cache.intern(labels),
            ));
        }
    }

    Ok(metrics)
}

fn read_u64(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_string(path: &str) -> Option<String> {
    Some(std::fs::read_to_string(path).ok()?.trim().to_string())
}
