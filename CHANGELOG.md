# Changelog

## 0.3.0 (unreleased)

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
