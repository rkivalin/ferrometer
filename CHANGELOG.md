# Changelog

## 0.1.0 (unreleased)

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
