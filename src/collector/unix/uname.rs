use nix::sys::utsname::uname;

use crate::error::Result;
use crate::signal::{Labels, Metric};

pub fn collect() -> Result<Vec<Metric>> {
    let info =
        uname().map_err(|e| crate::error::Error::Collector(format!("uname: {e}")))?;

    let mut labels = Labels::new();
    labels.insert("sysname".into(), info.sysname().to_string_lossy().into());
    labels.insert("release".into(), info.release().to_string_lossy().into());
    labels.insert("version".into(), info.version().to_string_lossy().into());
    labels.insert("machine".into(), info.machine().to_string_lossy().into());
    labels.insert("nodename".into(), info.nodename().to_string_lossy().into());

    Ok(vec![Metric::gauge("node_uname_info", 1.0, labels)])
}
