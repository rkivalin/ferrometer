# Changelog

## 0.1.0 (unreleased)

### Features

- Unix system metrics collector (CPU, memory, disk, filesystem, network, load, uname)
- Prometheus scraper collector (pull text-exposition endpoints)
- Journald log source + Loki log sink (tail systemd journal → push to Grafana Loki)
- OTLP HTTP forwarder with gzip compression, basic auth, in-memory ring buffer
  for the duration of a remote outage, and exponential backoff
- Feature-flagged components; `collector-unix`, `collector-prometheus`,
  `forwarder-otlphttp`, `log-source-journald-systemd`, `log-sink-loki`
  all enabled by default
- TOML configuration with validation; string values support
  `${instance.name}`, `${version}`, and `${env:VAR}` placeholders, with
  hard errors on unset env vars
- Hardened systemd service unit running under a transient `DynamicUser`
- Arch Linux and Debian packaging
