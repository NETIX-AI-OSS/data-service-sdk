# data-service-sdk

Python SDK for data-service processing: input evaluation, subscription consumption, output routing, worker deployment, and Redis timeseries-backed state.

## Install
```bash
pip install data-service-sdk
```

## Import
```python
from data_service_sdk.handlers.output.manager import OutputManager
from data_service_sdk.worker.deployment.deployment_handler import WorkerHandler
```

`data_service_sdk.*` is the public import namespace for `1.0.0`. The existing `framework.*` namespace remains available as a compatibility layer for current integrations.

## Development
```bash
uv sync
uv run pytest
uv run mypy --config-file mypy.ini framework tests
uv run black framework tests
```

## Documentation
- Full docs index: [docs/index.md](docs/index.md)

## Notes
- Root `README.md` is intentionally kept as package metadata readme (`pyproject.toml` -> `readme = "README.md"`).
- All detailed technical docs live under `docs/`.
