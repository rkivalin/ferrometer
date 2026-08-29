use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;

use crate::collector::label_cache::LabelCache;
use crate::error::Result;
use crate::signal::{Labels, Metric};

/// Hardware monitoring sensors from `/sys/class/hwmon` — temperatures, fans,
/// voltages, currents, power and humidity, whatever the host's drivers
/// register. All of it is world-readable, so no privileges and no daemon: NVMe
/// drives register a chip from the `nvme` driver itself, and SATA/SAS drives do
/// once the `drivetemp` module is loaded (it does not autoload). That covers
/// what the long-dead `hddtemp` daemon was for, alongside CPU, DIMM and GPU
/// sensors from the same loop.
///
/// Metric names follow node_exporter's hwmon collector:
/// `node_hwmon_<type>[_<property>][_<unit>]`, e.g. `node_hwmon_temp_celsius`
/// (a `temp1_input` reading) or `node_hwmon_temp_crit_celsius`. Series carry an
/// extra `disk` label when the chip belongs to a block device, so a drive's
/// temperature joins directly with `node_disk_*` and `node_md_disk_*`.
pub fn collect(cache: &mut LabelCache, filter: &Regex) -> Result<Vec<Metric>> {
    let Ok(entries) = fs::read_dir("/sys/class/hwmon") else {
        return Ok(Vec::new());
    };
    let mut chips: Vec<PathBuf> = entries.flatten().map(|entry| entry.path()).collect();
    chips.sort();

    let disks = block_devices_by_path();
    let mut metrics = Vec::new();
    for chip in chips {
        collect_chip(cache, &chip, &disks, filter, &mut metrics);
    }
    Ok(metrics)
}

/// One sensor class: the sysfs file prefix, the divisor taking the kernel's
/// integer units to the base unit, and the name of that unit. An empty unit
/// means the metric name carries none (`node_hwmon_humidity`).
struct SensorType {
    prefix: &'static str,
    scale: f64,
    unit: &'static str,
    counter: bool,
}

const SENSOR_TYPES: &[SensorType] = &[
    SensorType {
        prefix: "temp",
        scale: 1000.0,
        unit: "celsius",
        counter: false,
    },
    SensorType {
        prefix: "fan",
        scale: 1.0,
        unit: "rpm",
        counter: false,
    },
    SensorType {
        prefix: "in",
        scale: 1000.0,
        unit: "volts",
        counter: false,
    },
    SensorType {
        prefix: "curr",
        scale: 1000.0,
        unit: "amps",
        counter: false,
    },
    SensorType {
        prefix: "power",
        scale: 1_000_000.0,
        unit: "watt",
        counter: false,
    },
    SensorType {
        prefix: "energy",
        scale: 1_000_000.0,
        unit: "joule",
        counter: true,
    },
    SensorType {
        prefix: "humidity",
        scale: 1.0,
        unit: "",
        counter: false,
    },
];

/// Sensor properties worth exporting, and whether the value carries the
/// sensor's unit — an alarm is a latched 0/1 flag, not a reading. Everything
/// else hwmon exposes (hysteresis, enable toggles, pwm curves, fault counts)
/// is skipped to keep the series count bounded.
/// Unset temperature limits, in the millidegrees hwmon reports. NVMe encodes
/// thresholds as a u16 of Kelvin, so "no limit configured" arrives as 0 K or
/// 65535 K — a -273.15 C floor and a 65261.85 C ceiling. No hardware has
/// those, and left in they wreck an auto-scaled y-axis and any
/// `input / max` headroom ratio, so drop them rather than export a fiction.
const TEMP_SENTINELS: [f64; 2] = [-273_150.0, 65_261_850.0];

const PROPERTIES: &[(&str, bool)] = &[
    ("input", true),
    ("min", true),
    ("max", true),
    ("crit", true),
    ("alarm", false),
    ("crit_alarm", false),
];

fn collect_chip(
    cache: &mut LabelCache,
    dir: &Path,
    disks: &HashMap<PathBuf, String>,
    filter: &Regex,
    out: &mut Vec<Metric>,
) {
    let device = fs::canonicalize(dir.join("device")).ok();
    let chip = chip_name(dir, device.as_deref());
    if !filter.is_match(&chip) {
        return;
    }
    let disk = device.as_deref().and_then(|path| disks.get(path));

    let labels = |extra: &[(&str, &str)]| {
        let mut l = Labels::new();
        l.insert("chip".into(), chip.clone());
        if let Some(disk) = disk {
            l.insert("disk".into(), disk.clone());
        }
        for (k, v) in extra {
            l.insert((*k).into(), (*v).to_string());
        }
        l
    };

    // The driver's own name for the chip (`nvme`, `coretemp`, `drivetemp`) —
    // useful for selecting a class of sensor across hosts, where the chip
    // identity is per-host.
    if let Some(name) = read_string(&dir.join("name")) {
        out.push(Metric::gauge(
            "node_hwmon_chip_names",
            1.0,
            cache.intern(labels(&[("chip_name", &name)])),
        ));
    }

    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut files: Vec<String> = entries
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();
    files.sort();

    for file in files {
        let Some((sensor, property)) = split_attr(&file) else {
            continue;
        };
        let Some(sensor_type) = sensor_type(sensor) else {
            continue;
        };

        // The human-readable name the driver gives this sensor ("Composite",
        // "Core 0", "Package id 0"). Emitted as its own series, as
        // node_exporter does, since it is a string.
        if property == "label" {
            if let Some(label) = read_string(&dir.join(&file)) {
                out.push(Metric::gauge(
                    "node_hwmon_sensor_label",
                    1.0,
                    cache.intern(labels(&[("sensor", sensor), ("label", &label)])),
                ));
            }
            continue;
        }

        let Some(&(_, scaled)) = PROPERTIES.iter().find(|(p, _)| *p == property) else {
            continue;
        };
        let Some(raw) = read_f64(&dir.join(&file)) else {
            continue;
        };
        if sensor_type.prefix == "temp" && TEMP_SENTINELS.contains(&raw) {
            continue;
        }
        let value = if scaled { raw / sensor_type.scale } else { raw };

        let name = metric_name(sensor_type, property, scaled);
        let labels = cache.intern(labels(&[("sensor", sensor)]));
        out.push(if sensor_type.counter && property == "input" {
            Metric::counter(&name, value, labels)
        } else {
            Metric::gauge(&name, value, labels)
        });
    }
}

/// `node_hwmon_temp_celsius`, `node_hwmon_temp_crit_celsius`,
/// `node_hwmon_temp_alarm`, `node_hwmon_energy_joule_total`, ...
fn metric_name(sensor_type: &SensorType, property: &str, scaled: bool) -> String {
    let mut name = format!("node_hwmon_{}", sensor_type.prefix);
    if property != "input" {
        name.push('_');
        name.push_str(property);
    }
    if scaled && !sensor_type.unit.is_empty() {
        name.push('_');
        name.push_str(sensor_type.unit);
    }
    if sensor_type.counter && property == "input" {
        name.push_str("_total");
    }
    name
}

/// Stable identifier for a chip. hwmon index numbers are handed out at probe
/// time and shuffle across reboots, so they cannot be the identity: prefer the
/// device the sensors hang off, named `<bus>_<device>` (`nvme_nvme0`,
/// `platform_coretemp_0`), then the driver `name`, then the directory itself.
fn chip_name(dir: &Path, device: Option<&Path>) -> String {
    if let Some(device) = device {
        let name = device.file_name().and_then(|n| n.to_str());
        let bus = device
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str());
        match (bus, name) {
            (Some(bus), Some(name)) => return sanitize(&format!("{bus}_{name}")),
            (None, Some(name)) => return sanitize(name),
            _ => {}
        }
    }
    if let Some(name) = read_string(&dir.join("name")) {
        return sanitize(&name);
    }
    dir.file_name()
        .and_then(|n| n.to_str())
        .map(sanitize)
        .unwrap_or_default()
}

/// Canonical sysfs device path → block device name, for devices that map to
/// exactly one entry under `/sys/block`. An NVMe controller with several
/// namespaces is ambiguous about which disk its sensors describe, so it gets
/// no `disk` label rather than an arbitrary one.
fn block_devices_by_path() -> HashMap<PathBuf, String> {
    let Ok(entries) = fs::read_dir("/sys/block") else {
        return HashMap::new();
    };
    let mut found: HashMap<PathBuf, Vec<String>> = HashMap::new();
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let Ok(device) = fs::canonicalize(entry.path().join("device")) else {
            continue;
        };
        found.entry(device).or_default().push(name);
    }
    found
        .into_iter()
        .filter_map(|(path, disks)| match disks.as_slice() {
            [disk] => Some((path, disk.clone())),
            _ => None,
        })
        .collect()
}

/// Split an attribute filename such as `temp1_crit_alarm` into its sensor
/// (`temp1`) and property (`crit_alarm`).
fn split_attr(file: &str) -> Option<(&str, &str)> {
    let (sensor, property) = file.split_once('_')?;
    (!property.is_empty()).then_some((sensor, property))
}

/// Match a sensor name against the known classes: a class prefix followed by
/// an index, so `in0` is a voltage but `intrusion0` is not.
fn sensor_type(sensor: &str) -> Option<&'static SensorType> {
    SENSOR_TYPES.iter().find(|t| {
        sensor
            .strip_prefix(t.prefix)
            .is_some_and(|index| !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit()))
    })
}

/// Sysfs device names carry `.` and `:`; fold them so chip labels read the
/// same way as node_exporter's.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn read_f64(path: &Path) -> Option<f64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_string(path: &Path) -> Option<String> {
    Some(fs::read_to_string(path).ok()?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stype(prefix: &str) -> &'static SensorType {
        SENSOR_TYPES.iter().find(|t| t.prefix == prefix).unwrap()
    }

    #[test]
    fn attribute_splitting() {
        assert_eq!(split_attr("temp1_input"), Some(("temp1", "input")));
        assert_eq!(
            split_attr("temp1_crit_alarm"),
            Some(("temp1", "crit_alarm"))
        );
        assert_eq!(split_attr("name"), None);
        assert_eq!(split_attr("temp1_"), None);
    }

    #[test]
    fn sensor_classes() {
        assert_eq!(sensor_type("temp1").map(|t| t.prefix), Some("temp"));
        assert_eq!(sensor_type("in0").map(|t| t.prefix), Some("in"));
        assert_eq!(sensor_type("energy1").map(|t| t.prefix), Some("energy"));
        // Not sensor readings: no index, or a different attribute that merely
        // starts with a class prefix.
        assert!(sensor_type("intrusion0").is_none());
        assert!(sensor_type("temp").is_none());
        assert!(sensor_type("pwm1").is_none());
    }

    #[test]
    fn metric_names() {
        assert_eq!(
            metric_name(stype("temp"), "input", true),
            "node_hwmon_temp_celsius"
        );
        assert_eq!(
            metric_name(stype("temp"), "crit", true),
            "node_hwmon_temp_crit_celsius"
        );
        assert_eq!(
            metric_name(stype("temp"), "crit_alarm", false),
            "node_hwmon_temp_crit_alarm"
        );
        assert_eq!(
            metric_name(stype("fan"), "input", true),
            "node_hwmon_fan_rpm"
        );
        assert_eq!(
            metric_name(stype("fan"), "min", true),
            "node_hwmon_fan_min_rpm"
        );
        assert_eq!(
            metric_name(stype("humidity"), "input", true),
            "node_hwmon_humidity"
        );
        assert_eq!(
            metric_name(stype("energy"), "input", true),
            "node_hwmon_energy_joule_total"
        );
    }

    #[test]
    fn unset_nvme_temperature_limits_are_sentinels() {
        // 0 K and 65535 K, as the nvme driver reports an unconfigured limit.
        assert!(TEMP_SENTINELS.contains(&-273_150.0));
        assert!(TEMP_SENTINELS.contains(&65_261_850.0));
        // A real limit nearby must survive.
        assert!(!TEMP_SENTINELS.contains(&81_850.0));
    }

    #[test]
    fn chip_names_prefer_the_device_over_the_hwmon_index() {
        assert_eq!(
            chip_name(
                Path::new("/sys/class/hwmon/hwmon1"),
                Some(Path::new("/sys/devices/pci0000:00/0000:00:06.0/nvme/nvme0")),
            ),
            "nvme_nvme0"
        );
        assert_eq!(
            chip_name(
                Path::new("/sys/class/hwmon/hwmon8"),
                Some(Path::new("/sys/devices/platform/coretemp.0")),
            ),
            "platform_coretemp_0"
        );
    }
}
