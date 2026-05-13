use std::collections::HashMap;

use nix::sys::statvfs::statvfs;
use procfs::process::{MountInfo, Process};
use regex::Regex;

use crate::collector::label_cache::LabelCache;
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

pub async fn collect(
    cache: &mut LabelCache,
    mount_filter: &Regex,
    type_filter: &Regex,
    dedupe_devices: bool,
) -> Result<Vec<Metric>> {
    // /proc/self/mountinfo carries the per-mount root and major:minor that
    // /proc/mounts lacks — both are required to identify bind mounts.
    let mounts = Process::myself()
        .and_then(|p| p.mountinfo())
        .map_err(|e| crate::error::Error::Collector(format!("filesystem: {e}")))?;

    let mut candidates: Vec<MountInfo> = mounts
        .into_iter()
        .filter(|m| !IGNORED_FS_TYPES.contains(&m.fs_type.as_str()))
        .filter(|m| type_filter.is_match(&m.fs_type))
        .filter(|m| {
            m.mount_point
                .to_str()
                .is_some_and(|s| mount_filter.is_match(s))
        })
        .collect();

    if dedupe_devices {
        let mut by_dev: HashMap<String, MountInfo> = HashMap::new();
        for m in candidates {
            match by_dev.get(&m.majmin) {
                Some(existing) if !is_more_canonical(&m, existing) => {}
                _ => {
                    by_dev.insert(m.majmin.clone(), m);
                }
            }
        }
        candidates = by_dev.into_values().collect();
    }

    let mut metrics = Vec::new();
    for mount in candidates {
        // statvfs can block for extended periods on stale network mounts
        // (NFS, SSHFS, etc). Move it off the async runtime so a single hung
        // mount can't freeze the collector or forwarder tasks.
        let path = mount.mount_point.clone();
        let stat = match tokio::task::spawn_blocking(move || statvfs(&path)).await {
            Ok(Ok(s)) => s,
            _ => continue,
        };

        let frsize = stat.fragment_size() as f64;
        let mountpoint = match mount.mount_point.to_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let device = mount.mount_source.clone().unwrap_or_default();

        let mut labels = Labels::new();
        labels.insert("device".into(), device);
        labels.insert("mountpoint".into(), mountpoint);
        labels.insert("fstype".into(), mount.fs_type.clone());
        let labels = cache.intern(labels);

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

// Among mounts sharing a major:minor, the entry with root="/" is the actual
// filesystem mount and the others are bind mounts of subtrees. Ties (whole-fs
// binds, btrfs subvolumes) break by lexicographic mount_point so the choice
// is stable across runs regardless of mount ordering.
fn is_more_canonical(candidate: &MountInfo, current: &MountInfo) -> bool {
    let cand_root = candidate.root == "/";
    let curr_root = current.root == "/";
    match (cand_root, curr_root) {
        (true, false) => true,
        (false, true) => false,
        _ => candidate.mount_point < current.mount_point,
    }
}
