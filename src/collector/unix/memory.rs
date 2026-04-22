use procfs::Current;

use crate::error::Result;
use crate::signal::{Labels, Metric};

pub fn collect() -> Result<Vec<Metric>> {
    let info = procfs::Meminfo::current()
        .map_err(|e| crate::error::Error::Collector(format!("memory: {e}")))?;
    let mut metrics = Vec::new();

    let fields: &[(&str, Option<u64>)] = &[
        ("node_memory_MemTotal_bytes", Some(info.mem_total)),
        ("node_memory_MemFree_bytes", Some(info.mem_free)),
        ("node_memory_MemAvailable_bytes", info.mem_available),
        ("node_memory_Buffers_bytes", Some(info.buffers)),
        ("node_memory_Cached_bytes", Some(info.cached)),
        ("node_memory_SwapTotal_bytes", Some(info.swap_total)),
        ("node_memory_SwapFree_bytes", Some(info.swap_free)),
        ("node_memory_Shmem_bytes", info.shmem),
        ("node_memory_SReclaimable_bytes", info.s_reclaimable),
        ("node_memory_SUnreclaim_bytes", info.s_unreclaim),
    ];

    for &(name, value) in fields {
        if let Some(v) = value {
            metrics.push(Metric::gauge(name, (v * 1024) as f64, Labels::new()));
        }
    }

    Ok(metrics)
}
