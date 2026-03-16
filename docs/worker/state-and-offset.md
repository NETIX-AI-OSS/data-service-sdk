# Worker State and Offset

## Purpose
Store and retrieve worker state values in Redis Timeseries and manage polling offsets.

## Key Classes and Functions
- `data_service_sdk.worker.state_handler.state_handler.StateHandler`
- `StateHandler.get_state(...)`, `StateHandler.set_state(...)`
- `StateHandler.get_worker_state_ts_id(...)`
- `data_service_sdk.worker.state_handler.state_handler.WorkerOffsetHandler`

## Minimal Configuration Example
```python
from data_service_sdk.worker.state_handler.state_handler import StateHandler, WorkerOffsetHandler

state = StateHandler()
state.set_state("worker-12", "health", "ok")
health = state.get_state("worker-12", "health", default="unknown")

offset_handler = WorkerOffsetHandler()
offset = offset_handler.get_offset("worker-12", retention_ts_sec=60)
offset_handler.set_offset("worker-12", offset)
```

## Known Constraints and Edge Cases
- State keys are stored as `<worker_id>/<state>` timeseries IDs.
- `get_state` returns `default` if no data points exist.
- Current `get_offset` returns current epoch milliseconds and does not enforce retention-based rewind logic (legacy logic is commented out).

## Related Links
- [Timeseries Utilities](../utils/timeseries.md)
- [Worker Deployment](deployment.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
