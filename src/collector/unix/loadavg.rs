use procfs::Current;

use crate::collector::unix::LabelCache;
use crate::error::Result;
use crate::signal::{Labels, Metric};

pub fn collect(cache: &mut LabelCache) -> Result<Vec<Metric>> {
    let load = procfs::LoadAverage::current()
        .map_err(|e| crate::error::Error::Collector(format!("loadavg: {e}")))?;
    let labels = cache.intern(Labels::new());

    Ok(vec![
        Metric::gauge("node_load1", load.one.into(), labels.clone()),
        Metric::gauge("node_load5", load.five.into(), labels.clone()),
        Metric::gauge("node_load15", load.fifteen.into(), labels),
    ])
}
