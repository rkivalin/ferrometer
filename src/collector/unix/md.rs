use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::collector::label_cache::LabelCache;
use crate::error::Result;
use crate::signal::{Labels, Metric};

/// md (Linux software RAID) metrics, read from `/sys/block/md*/md/`.
///
/// Everything touched here is world-readable, so the collector works
/// unprivileged inside the systemd sandbox; `mdadm --detail` would need root
/// and a subprocess to report the same state. `/proc/mdstat` is consulted only
/// as an existence check — it is absent when the md module isn't loaded, which
/// is the common case, and the collector then costs a single `stat`.
///
/// Metric names follow node_exporter's mdadm collector where it has an
/// equivalent. `node_md_degraded`, `node_md_mismatch_cnt`,
/// `node_md_sync_speed_bytes`, `node_md_chunk_size_bytes`, `node_md_info` and
/// the `node_md_disk_*` family have none: node_exporter parses `/proc/mdstat`,
/// which doesn't carry them.
pub fn collect(cache: &mut LabelCache) -> Result<Vec<Metric>> {
    if !Path::new("/proc/mdstat").exists() {
        return Ok(Vec::new());
    }

    let mut metrics = Vec::new();
    for (device, md) in arrays() {
        collect_array(cache, &device, &md, &mut metrics);
    }
    Ok(metrics)
}

/// The activity states node_exporter reports, plus `repair` and `reshape`
/// which sysfs distinguishes and `/proc/mdstat` does not.
const MD_STATES: &[&str] = &[
    "active",
    "inactive",
    "recovering",
    "resync",
    "check",
    "repair",
    "reshape",
];

fn collect_array(cache: &mut LabelCache, device: &str, md: &Path, out: &mut Vec<Metric>) {
    let labels = |extra: &[(&str, &str)]| {
        let mut l = Labels::new();
        l.insert("device".into(), device.to_string());
        for (k, v) in extra {
            l.insert((*k).into(), (*v).to_string());
        }
        l
    };

    // Identity, as a gauge of 1 with everything descriptive as labels. Only
    // attributes the array actually exposes are included — a container device
    // holding external metadata has no level or chunk size.
    let mut info = labels(&[]);
    for attr in ["level", "metadata_version", "uuid", "consistency_policy"] {
        if let Some(value) = read_string(&md.join(attr)) {
            info.insert(attr.into(), value);
        }
    }
    out.push(Metric::gauge("node_md_info", 1.0, cache.intern(info)));

    for (attr, name) in [
        // Number of members missing or failed. 0 on a healthy array — this is
        // the series to alert on.
        ("degraded", "node_md_degraded"),
        ("raid_disks", "node_md_disks_required"),
        // Sectors that disagreed during the last check. Non-zero means the
        // mirrors/parity diverged; only meaningful once a scrub has run.
        ("mismatch_cnt", "node_md_mismatch_cnt"),
        ("chunk_size", "node_md_chunk_size_bytes"),
    ] {
        if let Some(value) = read_u64(&md.join(attr)) {
            out.push(Metric::gauge(name, value as f64, cache.intern(labels(&[]))));
        }
    }

    // `/sys/block/<dev>/size` counts 512-byte sectors; node_md_blocks is in the
    // 1K blocks /proc/mdstat reports, which is what node_exporter exposes.
    let blocks = md
        .parent()
        .and_then(|dev| read_u64(&dev.join("size")))
        .map(|sectors| sectors / 2);
    if let Some(blocks) = blocks {
        out.push(Metric::gauge(
            "node_md_blocks",
            blocks as f64,
            cache.intern(labels(&[])),
        ));
    }

    // While a sync runs, `sync_completed` is "<done> / <total>" in sectors;
    // otherwise it reads "none" or "delayed" and node_exporter reports
    // blocks_synced == blocks, so mirror that.
    let completed = read_string(&md.join("sync_completed"));
    let synced = completed
        .as_deref()
        .and_then(parse_sync_completed)
        .map(|(done, _)| done / 2)
        .or(blocks);
    if let Some(synced) = synced {
        out.push(Metric::gauge(
            "node_md_blocks_synced",
            synced as f64,
            cache.intern(labels(&[])),
        ));
    }

    // Also "none" when idle. Reported as 0 rather than dropped so a rebuild
    // rate panel doesn't gap between syncs. Kernel units are KiB/s.
    if let Some(raw) = read_string(&md.join("sync_speed")) {
        let kib = raw.parse::<u64>().unwrap_or(0);
        out.push(Metric::gauge(
            "node_md_sync_speed_bytes",
            (kib * 1024) as f64,
            cache.intern(labels(&[])),
        ));
    }

    let array_state = read_string(&md.join("array_state"));
    let sync_action = read_string(&md.join("sync_action"));
    if let Some(current) = activity_state(array_state.as_deref(), sync_action.as_deref()) {
        // Every state every tick with a 0/1 value, as node_exporter does, so
        // an alert rule never faces an absent series.
        for state in MD_STATES {
            let value = if *state == current { 1.0 } else { 0.0 };
            out.push(Metric::gauge(
                "node_md_state",
                value,
                cache.intern(labels(&[("state", state)])),
            ));
        }
    }

    // Whether the last scrub or rebuild ran to completion; "idle" until one
    // has finished since boot. Single series with the action as a label, as
    // the cpu collector does for the scaling governor.
    if let Some(action) = read_string(&md.join("last_sync_action")) {
        out.push(Metric::gauge(
            "node_md_last_sync_action",
            1.0,
            cache.intern(labels(&[("action", &action)])),
        ));
    }

    let mut tally = BTreeMap::from([("active", 0u64), ("failed", 0), ("spare", 0)]);
    for (disk, dir) in members(md) {
        let disk_labels = |extra: &[(&str, &str)]| {
            let mut l = Labels::new();
            l.insert("device".into(), device.to_string());
            l.insert("disk".into(), disk.clone());
            for (k, v) in extra {
                l.insert((*k).into(), (*v).to_string());
            }
            l
        };

        if let Some(state) = read_string(&dir.join("state")) {
            *tally.entry(disk_bucket(&state)).or_default() += 1;
            // The raw sysfs flags, one series each: in_sync, faulty, spare,
            // write_mostly, blocked, want_replacement, journal, ...
            for flag in state.split(',').map(str::trim).filter(|f| !f.is_empty()) {
                out.push(Metric::gauge(
                    "node_md_disk_state",
                    1.0,
                    cache.intern(disk_labels(&[("state", flag)])),
                ));
            }
        }

        // Read errors md corrected without evicting the device. A rising count
        // is an early warning that SMART often misses.
        if let Some(errors) = read_u64(&dir.join("errors")) {
            out.push(Metric::counter(
                "node_md_disk_errors_total",
                errors as f64,
                cache.intern(disk_labels(&[])),
            ));
        }

        if let Some(kib) = read_u64(&dir.join("size")) {
            out.push(Metric::gauge(
                "node_md_disk_size_bytes",
                (kib * 1024) as f64,
                cache.intern(disk_labels(&[])),
            ));
        }

        // "none" for a spare holding no raid role; -1 keeps the series present
        // across a member going spare and back.
        if let Some(raw) = read_string(&dir.join("slot")) {
            let slot = raw.parse::<i64>().unwrap_or(-1);
            out.push(Metric::gauge(
                "node_md_disk_slot",
                slot as f64,
                cache.intern(disk_labels(&[])),
            ));
        }

        // One line per bad range, empty on a healthy member.
        for (attr, name) in [
            ("bad_blocks", "node_md_disk_bad_blocks"),
            (
                "unacknowledged_bad_blocks",
                "node_md_disk_unacknowledged_bad_blocks",
            ),
        ] {
            if let Some(content) = read_string(&dir.join(attr)) {
                let count = content.lines().filter(|l| !l.trim().is_empty()).count();
                out.push(Metric::gauge(
                    name,
                    count as f64,
                    cache.intern(disk_labels(&[])),
                ));
            }
        }
    }

    // Emitted unconditionally, zeros included, so `node_md_disks{state="failed"}
    // > 0` sees a series while the array is still healthy.
    for (state, count) in tally {
        out.push(Metric::gauge(
            "node_md_disks",
            count as f64,
            cache.intern(labels(&[("state", state)])),
        ));
    }
}

/// Every `/sys/block/md*` that has an `md/` subdirectory, sorted for stable
/// emission order. The subdirectory is the real test — a name alone doesn't
/// prove the device is an array.
fn arrays() -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir("/sys/block") else {
        return Vec::new();
    };
    let mut found: Vec<(String, PathBuf)> = entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            if !name.starts_with("md") {
                return None;
            }
            let md = entry.path().join("md");
            md.is_dir().then_some((name, md))
        })
        .collect();
    found.sort();
    found
}

/// Member devices of one array, from its `dev-<name>` subdirectories.
fn members(md: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir(md) else {
        return Vec::new();
    };
    let mut found: Vec<(String, PathBuf)> = entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let disk = name.strip_prefix("dev-")?.to_string();
            Some((disk, entry.path()))
        })
        .collect();
    found.sort();
    found
}

/// `sync_completed` holds "<done> / <total>" in sectors while a sync runs and
/// "none" or "delayed" otherwise.
fn parse_sync_completed(raw: &str) -> Option<(u64, u64)> {
    let (done, total) = raw.split_once('/')?;
    Some((done.trim().parse().ok()?, total.trim().parse().ok()?))
}

/// Bucket a member's comma-separated state flags into the active / failed /
/// spare tally node_exporter reports as `node_md_disks`.
fn disk_bucket(state: &str) -> &'static str {
    let has = |flag: &str| state.split(',').any(|f| f.trim() == flag);
    if has("faulty") {
        "failed"
    } else if has("in_sync") {
        "active"
    } else {
        "spare"
    }
}

/// Collapse `array_state` and `sync_action` into the single activity state
/// node_exporter derives from `/proc/mdstat`. An inactive array wins over
/// whatever `sync_action` says, since nothing is running on it.
fn activity_state(array_state: Option<&str>, sync_action: Option<&str>) -> Option<&'static str> {
    match (array_state, sync_action) {
        (Some("inactive"), _) => Some("inactive"),
        (_, Some("resync")) => Some("resync"),
        (_, Some("recover")) => Some("recovering"),
        (_, Some("check")) => Some("check"),
        (_, Some("repair")) => Some("repair"),
        (_, Some("reshape")) => Some("reshape"),
        (Some(_), _) => Some("active"),
        (None, _) => None,
    }
}

fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_string(path: &Path) -> Option<String> {
    Some(fs::read_to_string(path).ok()?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_completed_parsing() {
        assert_eq!(parse_sync_completed("1024 / 4096"), Some((1024, 4096)));
        assert_eq!(parse_sync_completed("0/8"), Some((0, 8)));
        assert_eq!(parse_sync_completed("none"), None);
        assert_eq!(parse_sync_completed("delayed"), None);
        assert_eq!(parse_sync_completed(""), None);
    }

    #[test]
    fn disk_state_buckets() {
        assert_eq!(disk_bucket("in_sync"), "active");
        assert_eq!(disk_bucket("in_sync,write_mostly"), "active");
        assert_eq!(disk_bucket("faulty"), "failed");
        assert_eq!(disk_bucket("faulty,in_sync"), "failed");
        assert_eq!(disk_bucket("spare"), "spare");
        assert_eq!(disk_bucket(""), "spare");
    }

    #[test]
    fn activity_states() {
        assert_eq!(activity_state(Some("clean"), Some("idle")), Some("active"));
        assert_eq!(activity_state(Some("active"), Some("check")), Some("check"));
        assert_eq!(
            activity_state(Some("active"), Some("recover")),
            Some("recovering")
        );
        assert_eq!(
            activity_state(Some("inactive"), Some("resync")),
            Some("inactive")
        );
        assert_eq!(activity_state(None, None), None);
    }
}
