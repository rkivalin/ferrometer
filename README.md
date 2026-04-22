# ferrometer

Lightweight telemetry collector written in Rust. Collects system metrics and forwards them via OTLP HTTP to backends like VictoriaMetrics.

Designed as a minimal alternative to Grafana Alloy / OpenTelemetry Collector for hosts that only need basic system metrics collection and forwarding.

## Features

- **Unix system metrics**: CPU, memory, disk, filesystem, network, load average, uname
- **OTLP HTTP forwarding**: push metrics to any OTLP-compatible backend (VictoriaMetrics, Grafana Mimir, etc.)
- **Feature-flagged components**: build only what you need
- **Small footprint**: ~5MB binary, ~5-10MB RSS (vs 300-500MB for Alloy)
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
username = "write_myhost"
password-file = "/run/credentials/ferrometer/otlp-password"
```

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `collector-unix` | yes | Unix system metrics from /proc and /sys |
| `forwarder-otlphttp` | yes | OTLP HTTP metrics push |

Build a minimal binary:

```sh
cargo build --release --no-default-features --features collector-unix,forwarder-otlphttp
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

## License

MIT
