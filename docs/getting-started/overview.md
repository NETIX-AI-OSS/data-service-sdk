# Getting Started Overview

## Purpose
Provide a fast path to run, test, and reason about `data-service-sdk` locally.

## Key Classes and Functions
- Runtime entry points use the public `data_service_sdk.*` namespace:
- `data_service_sdk.handlers.*` for input/output/subscription orchestration.
- `data_service_sdk.worker.*` for worker lifecycle, deployment, and state.
- `data_service_sdk.utils.*` for expression and timeseries primitives.
- `framework.*` remains available as a compatibility namespace for existing code.

## Minimal Configuration Example
```bash
# Install dependencies and tooling
uv sync

# Run tests
uv run pytest

# Type-check
uv run mypy --config-file mypy.ini framework tests

# Format
uv run black framework tests
```

## Known Constraints and Edge Cases
- The project readme is package metadata (`pyproject.toml` uses `readme = "README.md"`), so root `README.md` must stay present.
- Worker/deployment flows rely on environment-backed integrations (Kafka, Redis, Postgres, Kubernetes).
- Repository currently tracks no `openapi/` directory; do not assume checked-in schemas unless explicitly added.

## Related Links
- [Documentation Index](../index.md)
- [Repository Structure](../architecture/repo-structure.md)
- [Coverage Map](../testing/coverage.md)
