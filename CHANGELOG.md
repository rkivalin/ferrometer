# Changelog

## 0.1.0 (unreleased)

### Features

- Unix system metrics collector (CPU, memory, disk, filesystem, network, load, uname)
- OTLP HTTP forwarder with gzip compression, basic auth, in-memory ring buffer
  for the duration of a remote outage, and exponential backoff
- Feature-flagged components (`collector-unix`, `forwarder-otlphttp`)
- TOML configuration with validation
- Systemd service unit
- Arch Linux and Debian packaging
