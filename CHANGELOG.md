# Changelog

> Reconstructed from git history on 2026-09-14. Entries before this date were derived from
> commit subjects between release tags rather than written at release time, so they summarise
> what changed but may not capture every user-visible detail. Entries from the next release
> onward are written as part of the release.

This project uses two-component release tags (`v1.5`) and three-component package versions
(`1.5.0`); the two refer to the same release.

## Unreleased

## 1.7.0 — 2026-10-14 (tag `v1.7`)

### Fixed

- `WorkerHandler.deploy` works with kubernetes 37. The SDK now pins k8s-utils-py v1.2, which
  builds `V1Deployment` whole and keeps the ConfigMap namespace in `metadata`. k8s-utils-py v1.0
  failed on 37's pydantic client with `spec Field required` and
  `body.namespace Extra inputs are not permitted`.

### Changed

- Worker deployment is also tested through the real k8s-utils-py and kubernetes client classes,
  with only the HTTP pool stubbed.
- The dev lock moves to kubernetes 37.0.1.

## 1.6.0 — 2026-09-27 (tag `v1.6`)

### Added

- A shared paho v2 MQTT client factory and long-lived publisher that recovers indefinitely by
  default. Recovery is bounded by `MQTT_GIVE_UP_SECS`; `MQTT_PUBLISH_GIVE_UP_SECS` and
  `MQTT_RECONNECT_GIVE_UP_SECS` remain as deprecated aliases.

### Changed

- Requires paho-mqtt `>=2,<3`.
- Collapsed multi-line comments to single-line across the package.
- Cleaned up docs, license headers and docstrings.
- CI: migrated `ubuntu-latest` jobs to the `build-only` runner.

## 1.5.0 — 2026-08-15 (tag `v1.5`)

### Fixed

- Redis cluster clients no longer strand on stale node addresses after a topology change.

## 1.4.0 — 2026-07-16 (tag `v1.4`)

### Added

- `Timeseries.delete()` and `Timeseries.delete_by_id()`.
- An environment-driven default retention for timeseries keys.

### Fixed

- Retention is now carried on `TS.ADD`, so auto-created keys are never left unbounded.
- Aligned `DummyTimeseries` attribute types with `Timeseries.retention_msecs` for mypy.

### Changed

- Declared the project venv for pyright in `pyproject.toml`.
- Added a mypy pre-commit hook.
- Added explicit workflow permissions to CI (security hardening).

## 1.3.0 — 2026-06-12 (tag `v1.3`)

### Fixed

- The MQTT consumer reconnects with backoff and gives up loudly rather than silently.

### Changed

- Kafka and MQTT handlers access message attributes directly instead of via redundant `getattr`.
- Dropped black; formatting is ruff-only.
- Removed redundant int casts flagged by CI mypy.

## 1.2.0 — 2026-03-23 (tag `v1.2`)

### Changed

- The MQTT handler reuses client instances and manages connections more carefully.
- Coverage configuration omits test files from coverage reports.

## 1.1.0 — 2026-03-23 (tag `v1.1`)

### Changed

- MQTT message retention is set to true in the consumer.

## 1.0.0 — 2026-03-16 (tag `v1.0`)

- Initial open-source release.
