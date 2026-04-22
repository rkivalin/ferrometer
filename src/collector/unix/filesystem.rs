use nix::sys::statvfs::statvfs;

use crate::error::Result;
use crate::signal::{Labels, Metric};

const IGNORED_FS_TYPES: &[&str] = &[
    "sysfs",
    "proc",
    "devtmpfs",
    "devpts",
    "tmpfs",
    "securityfs",
    "cgroup",
    "cgroup2",
    "pstore",
    "debugfs",
    "hugetlbfs",
    "mqueue",
    "configfs",
    "fusectl",
    "tracefs",
    "bpf",
    "nsfs",
    "ramfs",
    "rpc_pipefs",
    "nfsd",
    "efivarfs",
    "autofs",
    "binfmt_misc",
    "overlay",
];

pub fn collect() -> Result<Vec<Metric>> {
    let mounts = procfs::mounts()
        .map_err(|e| crate::error::Error::Collector(format!("filesystem: {e}")))?;
    let mut metrics = Vec::new();

    for mount in mounts {
        if IGNORED_FS_TYPES.contains(&mount.fs_vfstype.as_str()) {
            continue;
        }

        let stat = match statvfs(mount.fs_file.as_str()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let frsize = stat.fragment_size() as f64;
        let mut labels = Labels::new();
        labels.insert("device".into(), mount.fs_spec.clone());
        labels.insert("mountpoint".into(), mount.fs_file.clone());
        labels.insert("fstype".into(), mount.fs_vfstype.clone());

        metrics.push(Metric::gauge(
            "node_filesystem_size_bytes",
            stat.blocks() as f64 * frsize,
            labels.clone(),
        ));
        metrics.push(Metric::gauge(
            "node_filesystem_free_bytes",
            stat.blocks_free() as f64 * frsize,
            labels.clone(),
        ));
        metrics.push(Metric::gauge(
            "node_filesystem_avail_bytes",
            stat.blocks_available() as f64 * frsize,
            labels.clone(),
        ));
        metrics.push(Metric::gauge(
            "node_filesystem_files",
            stat.files() as f64,
            labels.clone(),
        ));
        metrics.push(Metric::gauge(
            "node_filesystem_files_free",
            stat.files_free() as f64,
            labels,
        ));
    }

    Ok(metrics)
}
