# Changelog

> Reconstructed from git history on 2026-09-14. Entries before this date were derived from
> commit subjects between release tags rather than written at release time, so they summarise
> what changed but may not capture every user-visible detail. Entries from the next release
> onward are written as part of the release.

This project uses two-component release tags (`v1.5`) and three-component package versions
(`1.5.0`); the two refer to the same release.

## Unreleased

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
