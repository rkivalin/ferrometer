# Changelog

## 0.5.0 (unreleased)

### Features

- New `hwmon` sub-collector on the unix collector, enabled by default, exporting `/sys/class/hwmon` sensors under node_exporter's names: `node_hwmon_temp_celsius`, `node_hwmon_fan_rpm`, `node_hwmon_in_volts`, `node_hwmon_curr_amps`, `node_hwmon_power_watt`, `node_hwmon_humidity`, `node_hwmon_energy_joule_total`, their `_min_`/`_max_`/`_crit_` threshold variants, the `_alarm`/`_crit_alarm` flags, plus `node_hwmon_sensor_label` and `node_hwmon_chip_names`. Series carry an extra `disk` label when the chip belongs to exactly one block device, so a drive temperature joins directly with `node_disk_*` and `node_md_disk_*` — NVMe drives work out of the box, SATA/SAS need the `drivetemp` module loaded. No daemon and no privileges: this replaces what the unmaintained `hddtemp` service was for. The `chip` label names the underlying device (`nvme_nvme0`, `platform_coretemp_0`) rather than the reboot-unstable `hwmonN` index, and unset NVMe temperature limits (the driver's 0 K / 65535 K sentinels) are dropped instead of exported as -273.15 C / 65261.85 C. New `hwmon-chips` include-regex narrows collection — a many-core `coretemp` chip is over a hundred series on its own.

- New `md` sub-collector on the unix collector, enabled by default, reporting Linux software-RAID state from `/sys/block/md*/md/`: `node_md_degraded`, `node_md_disks{state}`, `node_md_disks_required`, `node_md_state{state}`, `node_md_blocks`, `node_md_blocks_synced`, `node_md_sync_speed_bytes`, `node_md_mismatch_cnt` (silent-corruption count from the last scrub), `node_md_chunk_size_bytes`, `node_md_info`, `node_md_last_sync_action`, and a per-member family — `node_md_disk_state`, `node_md_disk_errors_total` (read errors md corrected without evicting the device), `node_md_disk_bad_blocks`, `node_md_disk_unacknowledged_bad_blocks`, `node_md_disk_size_bytes`, `node_md_disk_slot`. Names match node_exporter's mdadm collector where it has an equivalent; the rest come from sysfs attributes `/proc/mdstat` does not carry. Everything read is world-readable, so no extra privileges and no `mdadm` subprocess are needed. Hosts with no md array pay one `stat` per tick.

### Fixes

### Changes

## 0.4.0 (2026-08-19)

### Features

- Prometheus scraper emits scrape-health series every tick, as Prometheus itself does: `up` (1/0), `scrape_duration_seconds`, `scrape_samples_scraped`, labelled with the collector's static labels plus `scraper=<name>`. A down target is now visible as `up == 0` instead of a silently absent series. Scrape failures are logged on state transitions only — `warn` on down, `info` on recovery (with the number of failed ticks), `debug` per repeat — instead of one `warn` per tick.
- journald source: new `batch-max-bytes` option (default `1M` = 1,000,000 bytes; SI units, `MiB` etc. for binary) caps each batch by approximate encoded size in addition to the `batch-size` entry count. Applies to fresh batches and to the top-up of an in-flight batch during backoff.

### Fixes

- journald → Loki shipping could wedge permanently after a sink outage: the backlog was batched by entry count only, so a batch of large entries could exceed Loki's 4 MiB gRPC message limit (`ResourceExhausted`) and be retried verbatim forever. Batches are now also byte-capped (see `batch-max-bytes`).
- Log shipper recovers from payload-too-large rejections (HTTP 413, or an error body reporting a message-size limit such as Loki's `received message larger than max`): instead of retrying the same batch verbatim it re-sends in progressively smaller chunks, and drops — with an `error`-level log carrying the labels and message head — any single entry the sink refuses on its own. Covers pathological entries and server limits lower than `batch-max-bytes`.
- Log shipper no longer retries Loki HTTP 400 validation rejections (timestamp too old/new, line too long, bad labels, …) forever. Loki ingests the valid entries of such a request and drops the offending ones server-side, so the shipper now logs the rejection at `error` level with Loki's message and acks the batch.
- Loki/proxy error response bodies are collapsed to a single line (whitespace folded, capped at 300 chars) before logging, so `log ship failed` warnings no longer span multiple journal lines — Loki's bodies end with a newline and a proxy's 413 page is multi-line HTML.

### Changes

## 0.3.0 (2026-05-13)

### Features

- Filesystem collector dedupes bind mounts: by default only one entry per block device is reported (the one whose mount root is `/`, tie-broken by lexicographic mountpoint). New `filesystem-mount-points` and `filesystem-fs-types` include-regex options narrow further on top of the hardcoded pseudo-fs floor; `filesystem-dedupe-devices = false` restores the previous one-entry-per-mountpoint behavior. Reads `/proc/self/mountinfo` instead of `/proc/mounts`.

### Fixes

### Changes

- HTTP clients now validate server certificates against the OS trust store (via `rustls-platform-verifier`) instead of a baked-in Mozilla CA bundle. Admin-installed CAs in `/etc/ssl/certs` (or equivalent) are picked up automatically; the `ca-cert-file` config option continues to add a private CA on top. Drops the obsolete `webpki-roots` feature from the reqwest dependency.

## 0.2.0 (2026-04-26)

### Features

- Logs auto-format for journald when stderr is hooked to it (detected via the `JOURNAL_STREAM` env var systemd sets): timestamps dropped (journald has its own), ANSI colors dropped, syslog `<N>` priority prefix per line so journald assigns the right `PRIORITY` per entry. The human-friendly format is unchanged for foreground / piped runs.
- Unix collector emits `node_memory_Dirty_bytes`.

## 0.1.0 (2026-04-25)

Initial release.

### Collectors

- **Unix**: CPU times + frequency / governor / thermal throttle metrics; memory; per-device disk stats including discards and flushes; filesystem usage via `statvfs`; per-interface network stats; load average; uname.
- **Prometheus**: scrape any text-exposition `/metrics` endpoint on an interval. Counter / Gauge / Untyped samples are forwarded; Histogram / Summary are dropped.

### Log shipping

- **journald source**: tails the systemd journal from a persisted cursor, so restarts don't lose or duplicate entries. Configurable mapping from journal fields to either Loki stream labels or per-entry structured metadata. journald itself is the durable buffer during a sink outage — no in-memory queue.
- **Loki sink**: native protobuf + snappy push API.
- Per-shipper exponential backoff on send failures.

### Forwarders

- **OTLP HTTP**: gzip compression, in-memory ring buffer (default 100 000 metrics, ~20 MB worst case, ~2 hours of typical retention), exponential backoff, configurable OTLP `Resource` attributes.

### Auth and TLS

- Per-backend authentication: basic (username + password / password-file), bearer (token / token-file), or arbitrary `Authorization` header. All string fields accept `${env:VAR}`.
- mTLS with optional custom CA bundle on every HTTP client. Cert + key may share a PEM file or be split.

### Configuration

- TOML, with `${instance.name}`, `${version}`, and `${env:VAR}` placeholder expansion. Unset env vars and unknown placeholders fail at config load.
- `ferrometer validate` checks structure without resolving placeholders, so configs can be checked from a shell without the service's runtime environment in scope.

### Packaging

- Hardened `ferrometer.service` running under a transient `DynamicUser` with a full sandbox profile (`ProtectSystem=strict`, `PrivateTmp/Devices/IPC/Users`, `LockPersonality`, `MemoryDenyWriteExecute`, `SystemCallFilter=@system-service`, etc.). `systemd-analyze security` exposure 1.2.
- Native packages for Arch Linux (`PKGBUILD`) and Debian/Ubuntu (`debian/`).
