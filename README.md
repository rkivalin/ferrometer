# ferrometer

Lightweight telemetry collector written in Rust. Collects system metrics, tails the systemd journal, and forwards both to OTLP/Loki backends.

Designed as a minimal alternative to Grafana Alloy / OpenTelemetry Collector for hosts that only need basic system metrics and log shipping.

## Features

- **Unix system metrics**: CPU, memory, disk, filesystem, network, load average, uname
- **Prometheus scraper**: pull text-exposition `/metrics` endpoints on an interval
- **Journald log source**: tail the systemd journal from a persisted cursor
- **OTLP HTTP forwarder**: push metrics to any OTLP-compatible backend (VictoriaMetrics, Grafana Mimir, etc.)
- **Loki log sink**: push entries to Grafana Loki (native protobuf + snappy)
- **Feature-flagged components**: build only what you need
- **Small footprint**: ~5MB binary, ~5-10MB RSS (vs 300-500MB for Alloy)
- **Hardened systemd unit**: runs as a transient `DynamicUser` with a locked-down sandbox (security exposure 1.2 per `systemd-analyze security`)
- **TOML configuration**

## Installation

### From source

Requires Rust 1.85+ and `protoc` (protobuf compiler).

```sh
cargo install --path .
```

### Arch Linux

```sh
makepkg -si
```

### Debian/Ubuntu

```sh
dpkg -i ferrometer_0.1.0-1_amd64.deb
```

## Usage

```sh
# Validate configuration
ferrometer validate -c /etc/ferrometer/config.toml

# Start collecting and forwarding
ferrometer run -c /etc/ferrometer/config.toml

# With debug logging
ferrometer run -c /etc/ferrometer/config.toml -vv
```

## Configuration

See [examples/config.toml](examples/config.toml) for a complete reference.

```toml
[instance]
name = "myhost"

[metrics.collectors.system]
type = "unix"
interval = "15s"
disk-devices = "^(nvme\\d+n\\d+|sd[a-z]+)$"
net-devices = "^(eth|en|wl)"

[metrics.forwarders.victoriametrics]
type = "otlphttp"
endpoint = "http://metrics.example.com:8428/opentelemetry"
username = "ferrometer"
password-file = "/run/credentials/ferrometer.service/otlp-password"
```

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `collector-unix` | yes | Unix system metrics from `/proc` and `/sys` |
| `collector-prometheus` | yes | Pull Prometheus `/metrics` endpoints |
| `forwarder-otlphttp` | yes | OTLP HTTP metrics push |
| `log-source-journald-systemd` | yes | Tail the systemd journal (requires `libsystemd`) |
| `log-sink-loki` | yes | Push log batches to Grafana Loki |

Build a minimal metrics-only binary:

```sh
cargo build --release --no-default-features \
    --features collector-unix,forwarder-otlphttp
```

## Component taxonomy

| Term | Direction | Description |
|------|-----------|-------------|
| **Collector** | in | Gathers data locally (system metrics, logs) |
| **Scraper** | in | Pulls data from remote endpoints |
| **Processor** | through | Transforms, filters, relabels data |
| **Forwarder** | out | Pushes data to remote destinations |

## Metrics

Metric names follow [node_exporter](https://github.com/prometheus/node_exporter) conventions for compatibility with existing dashboards:

- `node_cpu_seconds_total` (counter, labels: cpu, mode)
- `node_memory_MemTotal_bytes`, `node_memory_MemFree_bytes`, ... (gauge)
- `node_disk_reads_completed_total`, `node_disk_read_bytes_total`, ... (counter, label: device)
- `node_filesystem_size_bytes`, `node_filesystem_free_bytes`, ... (gauge, labels: device, mountpoint, fstype)
- `node_network_receive_bytes_total`, `node_network_transmit_bytes_total`, ... (counter, label: device)
- `node_load1`, `node_load5`, `node_load15` (gauge)
- `node_uname_info` (gauge=1, labels: sysname, release, version, machine, nodename)

## Logs

With the default feature set, ferrometer can tail the systemd journal and push entries to Grafana Loki. Each `[logs.shippers.<name>]` entry in the config pairs one source with one sink. Restarts resume from the last acked position without gaps, and there is no in-memory buffer — journald itself is the durable buffer during a Loki outage.

See [examples/config.toml](examples/config.toml) for the full logs configuration.

## Running under systemd

The bundled `ferrometer.service` unit runs as a transient user inside a locked-down sandbox, so no setup beyond the package install is required.

### Providing passwords via systemd credentials

Each backend (OTLP, Loki, Prometheus scrapes) can authenticate with a password read from a file. The recommended way to manage that file is through systemd encrypted credentials — they are encrypted at rest in `/etc/credstore.encrypted/` and decrypted into a tmpfs at `/run/credentials/ferrometer/<name>` only for the duration of the service.

Encrypt a password and store it at rest:

```sh
install -d -m 0700 /etc/credstore.encrypted
systemd-creds encrypt - /etc/credstore.encrypted/ferrometer-otlp-password
```

Teach the unit to load that credential at start with a drop-in override (do not edit the shipped unit; drop-ins survive package upgrades):

```sh
systemctl edit ferrometer.service
```

```ini
[Service]
LoadCredentialEncrypted=otlp-password:/etc/credstore.encrypted/ferrometer-otlp-password
```

Then point `password-file` in `config.toml` at the decrypted path. The `${env:CREDENTIALS_DIRECTORY}` placeholder expands to systemd's per-service credentials directory, so the config stays portable:

```toml
password-file = "${env:CREDENTIALS_DIRECTORY}/otlp-password"
```

Repeat for any additional secrets (e.g. `loki-password`).

### Placeholders in config values

String values in `config.toml` may reference:

| Placeholder | Resolves to |
|-------------|-------------|
| `${instance.name}` | `[instance].name` from this file |
| `${version}` | ferrometer's own version |
| `${env:VAR}` | the value of environment variable `VAR` |

Unrecognised placeholders and unset env vars fail at config load with a clear message, rather than silently expanding to empty. `ferrometer validate` skips placeholder resolution so the config can be checked from a shell without the service's runtime env vars in scope.

## License

MIT
