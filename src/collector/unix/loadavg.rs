use procfs::Current;

use crate::error::Result;
use crate::signal::{Labels, Metric};

pub fn collect() -> Result<Vec<Metric>> {
    let load = procfs::LoadAverage::current()
        .map_err(|e| crate::error::Error::Collector(format!("loadavg: {e}")))?;

    Ok(vec![
        Metric::gauge("node_load1", load.one.into(), Labels::new()),
        Metric::gauge("node_load5", load.five.into(), Labels::new()),
        Metric::gauge("node_load15", load.fifteen.into(), Labels::new()),
    ])
}
