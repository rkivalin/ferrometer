# ferrometer

Lightweight telemetry agent written in Rust. Collects Unix system metrics, scrapes Prometheus endpoints, tails the systemd journal, and forwards both metrics and logs to OTLP and Loki backends.

A minimal alternative to Grafana Alloy and the OpenTelemetry Collector for hosts that only need basic system metrics and log shipping. Resident set sits around 10 MB PSS in steady state, vs. several hundred MB for the alternatives.

## Features

- **Unix system metrics** from `/proc` and `/sys` — CPU (incl. frequency / governor / thermal throttle), memory, disk (incl. discards and flushes), filesystem, network, load average, software RAID, hwmon sensors (drive / CPU / DIMM temperatures, fans, voltages), uname.
- **Prometheus scraper** for any text-exposition `/metrics` endpoint.
- **systemd journal source**, cursor-persisted across restarts so no entries get re-shipped or skipped. Journald is the durable buffer during a sink outage — no in-memory queue.
- **OTLP HTTP forwarder** (VictoriaMetrics, Grafana Mimir, etc.) with gzip compression, an in-memory ring buffer for outage tolerance, and exponential backoff.
- **Loki sink** speaking the native protobuf + snappy push API, with per-entry structured metadata.
- **Auth**: basic, bearer, or arbitrary `Authorization` header. Secrets via env vars, files, or systemd encrypted credentials.
- **mTLS** with optional custom CA bundle on every HTTP client.
- **TOML config** with `${env:VAR}`, `${instance.name}`, `${version}` placeholder expansion and load-time validation.
- **Hardened systemd unit** running as a transient `DynamicUser` inside a locked-down sandbox.

## Installation

### Arch Linux

```sh
makepkg -si
```

### Debian/Ubuntu

```sh
dpkg -i ferrometer_0.1.0-1_amd64.deb
```

### From source

Requires Rust 1.85+, `protoc`, `pkg-config`, and the libsystemd development headers.

```sh
cargo install --path .
```

## Quickstart

A working metrics-only config:

```toml
[instance]
name = "myhost"

[metrics.collectors.system]
type = "unix"

[metrics.forwarders.victoriametrics]
type = "otlphttp"
endpoint = "http://metrics.example.com:8428/opentelemetry"
```

Validate and run:

```sh
ferrometer validate -c /etc/ferrometer/config.toml
ferrometer run -c /etc/ferrometer/config.toml
ferrometer run -c /etc/ferrometer/config.toml -vv     # debug logging
```

If installed from a package, just `systemctl enable --now ferrometer`.

## Configuration

[examples/config.toml](examples/config.toml) is the full reference. The sections below cover the parts most users care about; every field shown is optional unless noted.

### Metrics

#### Unix collector

```toml
[metrics.collectors.system]
type = "unix"
interval = "15s"
disk-devices = "^(nvme\\d+n\\d+|sd[a-z]+|vd[a-z]+)$"   # regex filter
net-devices = "^(eth|en|wl|bond)"
filesystem-mount-points = ""                            # include-regex, empty = all
filesystem-fs-types = ""                                # include-regex, empty = all
filesystem-dedupe-devices = true                        # collapse bind mounts
hwmon-chips = ""                                        # include-regex, empty = all
collectors = ["cpu", "memory", "disk", "filesystem", "md", "hwmon", "netdev", "loadavg", "uname"]
```

Bind mounts and other multi-mount setups produce one entry per mountpoint in `/proc/self/mountinfo`. With `filesystem-dedupe-devices` on (default), only one entry per block device is reported — the entry whose `root` is `/` wins, with lexicographic mountpoint as a tie-breaker. A hardcoded floor always skips pseudo-filesystems (`proc`, `sysfs`, `tmpfs`, `cgroup`, ...) before any user filter applies.

Metric names follow [node_exporter](https://github.com/prometheus/node_exporter) conventions for dashboard compatibility:

| Sub-collector | Metrics |
|---------------|---------|
| `cpu` | `node_cpu_seconds_total` (counter, labels: `cpu`, `mode`); `node_cpu_scaling_frequency_hertz`, `node_cpu_frequency_min_hertz`, `node_cpu_frequency_max_hertz` (gauge, label: `cpu`); `node_cpu_scaling_governor` (gauge=1, labels: `cpu`, `governor`); `node_cpu_core_throttles_total` (counter, labels: `cpu`, `package`); `node_cpu_package_throttles_total` (counter, label: `package`) |
| `memory` | `node_memory_MemTotal_bytes`, `node_memory_MemFree_bytes`, `node_memory_MemAvailable_bytes`, ... (gauge) |
| `disk` | `node_disk_reads_completed_total`, `node_disk_read_bytes_total`, `node_disk_writes_completed_total`, `node_disk_written_bytes_total`, `node_disk_io_time_seconds_total`, `node_disk_discards_completed_total`, `node_disk_flush_requests_total`, ... (counter, label: `device`) |
| `filesystem` | `node_filesystem_size_bytes`, `node_filesystem_free_bytes`, `node_filesystem_avail_bytes`, `node_filesystem_files`, `node_filesystem_files_free` (gauge, labels: `device`, `mountpoint`, `fstype`) |
| `md` | `node_md_degraded`, `node_md_disks_required`, `node_md_blocks`, `node_md_blocks_synced`, `node_md_mismatch_cnt`, `node_md_sync_speed_bytes`, `node_md_chunk_size_bytes` (gauge, label: `device`); `node_md_disks` (gauge, labels: `device`, `state`=`active`/`failed`/`spare`); `node_md_state` (gauge 0/1, labels: `device`, `state`); `node_md_info` (gauge=1, labels: `device`, `level`, `metadata_version`, `uuid`, `consistency_policy`); `node_md_last_sync_action` (gauge=1, labels: `device`, `action`); `node_md_disk_state` (gauge=1, labels: `device`, `disk`, `state`); `node_md_disk_errors_total` (counter, labels: `device`, `disk`); `node_md_disk_bad_blocks`, `node_md_disk_unacknowledged_bad_blocks`, `node_md_disk_size_bytes`, `node_md_disk_slot` (gauge, labels: `device`, `disk`) |
| `hwmon` | `node_hwmon_temp_celsius`, `node_hwmon_fan_rpm`, `node_hwmon_in_volts`, `node_hwmon_curr_amps`, `node_hwmon_power_watt`, `node_hwmon_humidity` (gauge, labels: `chip`, `sensor`, plus `disk` where the chip is a drive); `node_hwmon_energy_joule_total` (counter); the `_min_`/`_max_`/`_crit_` threshold variants of each (e.g. `node_hwmon_temp_crit_celsius`); `node_hwmon_temp_alarm`, `node_hwmon_temp_crit_alarm` (gauge 0/1); `node_hwmon_sensor_label` (gauge=1, labels: `chip`, `sensor`, `label`); `node_hwmon_chip_names` (gauge=1, labels: `chip`, `chip_name`) |
| `netdev` | `node_network_receive_bytes_total`, `node_network_transmit_bytes_total`, `node_network_receive_packets_total`, ... (counter, label: `device`) |
| `loadavg` | `node_load1`, `node_load5`, `node_load15` (gauge) |
| `uname` | `node_uname_info` (gauge=1, labels: `sysname`, `release`, `version`, `machine`, `nodename`) |

The `md` sub-collector reads `/sys/block/md*/md/`, which is world-readable — no privileges beyond the sandbox, and no `mdadm` subprocess. It costs one `stat` on hosts without software RAID: an absent `/proc/mdstat` means the md module isn't loaded and the sub-collector returns immediately. Array I/O throughput is not part of it — `md0` appears in `/proc/diskstats`, so widen `disk-devices` (e.g. `^(nvme\\d+n\\d+|sd[a-z]+|vd[a-z]+|md\\d+)$`) to get it from the `disk` sub-collector.

The `hwmon` sub-collector reads `/sys/class/hwmon` — no daemon and no privileges, so drive temperatures need neither the (long-unmaintained) `hddtemp` service nor a `smartctl` subprocess. NVMe drives register a chip from the `nvme` driver automatically; **SATA/SAS drives need the `drivetemp` module, which does not autoload** — `modprobe drivetemp` plus a `/etc/modules-load.d/` entry. USB-attached disks are not reachable either way. The same loop picks up CPU, DIMM, GPU and chassis sensors, which is why `hwmon-chips` exists: a many-core `coretemp` chip alone accounts for well over a hundred series, and `hwmon-chips = "^nvme"` (or `"drivetemp"`) narrows collection to drives.

The `chip` label identifies the device the sensors hang off (`nvme_nvme0`, `platform_coretemp_0`) rather than the `hwmonN` index, which the kernel assigns at probe time and reshuffles across reboots. Unset NVMe temperature limits — the 0 K / 65535 K sentinels the driver reports as -273.15 C and 65261.85 C — are dropped rather than exported.

#### Prometheus scraper

Pulls a text-exposition endpoint on an interval. Counter / Gauge / Untyped samples are forwarded as-is; Histogram and Summary samples are dropped (ferrometer's internal model is currently Counter/Gauge only).

Every tick also emits the synthetic scrape-health series Prometheus itself would: `up` (1/0), `scrape_duration_seconds` and `scrape_samples_scraped`, labelled with the collector's static labels plus `scraper=<collector name>`. A down target therefore shows as `up == 0` rather than as a silently absent series — alert on `up == 0` instead of `absent(...)`. Scrape failures are logged on state transitions only (`warn` on down, `info` on recovery with the number of failed ticks; per-tick repeats at `debug`).

```toml
[metrics.collectors.haproxy]
type = "prometheus"
url = "http://127.0.0.1:9021/metrics"
interval = "15s"

[metrics.collectors.haproxy.static-labels]
job = "haproxy"
```

### Logs

Each `[logs.shippers.<name>]` entry pairs one source with one sink. The journald source seeks to the last acked cursor on start, so restarts don't lose or duplicate entries. Batches close at `batch-size` entries (default 1000) or roughly `batch-max-bytes` of payload (default `1M` = 1 MB, under Loki's 4 MiB request limit), whichever comes first; `batch-wait` (default 5s) bounds how long a partial batch is held. If the sink still rejects a request as too large (HTTP 413, or Loki's `received message larger than max`), the shipper splits the batch into smaller chunks rather than retrying it verbatim; an entry that is too large even on its own is dropped with an `error` log. A Loki HTTP 400 validation rejection (timestamp too old, line too long, …) is logged at `error` and acked — Loki ingests the valid entries of such a request and drops the rest server-side, so a retry could never succeed.

```toml
[logs.shippers.journal]

[logs.shippers.journal.source]
type = "journald"

[logs.shippers.journal.source.static-labels]
job = "ferrometer"

[logs.shippers.journal.sink]
type = "loki"
endpoint = "http://loki.example.com:3100"
```

Stream labels (low cardinality, become Loki streams) and structured metadata (per-entry, queryable, no new streams) can both be sourced from journal fields:

```toml
[logs.shippers.journal.source.labels]
unit = "_SYSTEMD_UNIT"
priority = "PRIORITY"

[logs.shippers.journal.source.metadata]
hostname = "_HOSTNAME"
pid = "_PID"
container = "CONTAINER_NAME"
```

### Authentication

Pick one method per HTTP backend (OTLP forwarder, Loki sink, Prometheus scraper):

```toml
# Basic auth
username = "myhost"
password-file = "${env:CREDENTIALS_DIRECTORY}/otlp-password"

# Bearer token
bearer-token-file = "${env:CREDENTIALS_DIRECTORY}/otlp-token"

# Arbitrary Authorization header (escape hatch)
authorization = "Bearer ${env:OTLP_TOKEN}"
```

Both `password` / `password-file` and `bearer-token` / `bearer-token-file` accept the credential inline as a string (typically with `${env:VAR}`) or loaded from a file.

### TLS

mTLS and a custom trust root, on any HTTP backend. Composes with any auth method:

```toml
client-cert-file = "/etc/ferrometer/client.pem"
client-key-file = "${env:CREDENTIALS_DIRECTORY}/client.key"
ca-cert-file = "/etc/ferrometer/internal-ca.pem"
```

`client-key-file` is optional — if omitted, the key is read from `client-cert-file` (bundled-PEM form). `ca-cert-file` alone is valid for trusting a private CA on the server cert without doing mTLS.

### OTLP resource attributes

Distinct from per-metric labels: these go on the OTLP `Resource` message and carry semantic-convention identifiers. Useful for receivers that distinguish resource-level identity from per-metric dimensions.

```toml
[metrics.forwarders.victoriametrics.resource-attributes]
"service.instance.id" = "${instance.name}"
"service.name" = "ferrometer"
"service.version" = "${version}"
```

### Placeholders

Every string-valued config field accepts:

| Placeholder | Resolves to |
|-------------|-------------|
| `${instance.name}` | `[instance].name` from this file |
| `${version}` | ferrometer's own version |
| `${env:VAR}` | the value of environment variable `VAR` |

Unset env vars and unknown placeholders fail at config load with a clear message — never silent expansion to empty. `ferrometer validate` skips placeholder resolution so the config can be checked from a shell without the service's runtime environment.

## Running under systemd

The bundled `ferrometer.service` runs as a transient user inside a locked-down sandbox (full hardening profile, see the unit file). No setup beyond the package install is required for the default config to come up.

### Providing secrets via systemd credentials

The recommended way to manage passwords, bearer tokens, and TLS keys: encrypt them at rest in `/etc/credstore.encrypted/` and let systemd decrypt them into a per-service tmpfs (`/run/credentials/ferrometer.service/`) only for the lifetime of the service.

Encrypt and store:

```sh
install -d -m 0700 /etc/credstore.encrypted
systemd-creds encrypt - /etc/credstore.encrypted/ferrometer-otlp-password
```

Wire it into the service via a drop-in (do not edit the shipped unit; drop-ins survive package upgrades):

```sh
systemctl edit ferrometer.service
```

```ini
[Service]
LoadCredentialEncrypted=otlp-password:/etc/credstore.encrypted/ferrometer-otlp-password
```

Reference the decrypted path from the config using the placeholder, which expands to systemd's per-service credentials directory and keeps the config portable:

```toml
password-file = "${env:CREDENTIALS_DIRECTORY}/otlp-password"
```

Repeat for additional secrets (Loki password, bearer tokens, mTLS keys, etc.).

## Building from source

```sh
cargo build --release
```

Requires Rust 1.85+, `protoc`, `pkg-config`, and the libsystemd development headers (`libsystemd-dev` on Debian/Ubuntu, included in `systemd-libs` on Arch).

### Feature flags

All five components are enabled by default. Disabling features mainly matters when you want to drop the libsystemd link — useful for minimal containers or systems without systemd:

```sh
cargo build --release --no-default-features \
    --features collector-unix,collector-prometheus,forwarder-otlphttp
```

| Feature | Default | Notes |
|---------|---------|-------|
| `collector-unix` | yes | Reads `/proc` and `/sys`. No external dependency. |
| `collector-prometheus` | yes | Pulls remote `/metrics` endpoints. |
| `forwarder-otlphttp` | yes | OTLP HTTP push. |
| `log-source-journald-systemd` | yes | Tails the systemd journal. **Requires libsystemd at runtime.** |
| `log-sink-loki` | yes | Pushes to Grafana Loki. |

## License

MIT
