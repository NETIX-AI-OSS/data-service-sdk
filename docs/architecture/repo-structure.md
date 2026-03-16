# Repository Structure

## Purpose
Describe the current tracked layout and responsibilities for each major package.

## Key Classes and Functions
- `framework/handlers/input`: `ExpressionInputController`, `StateMachineInputController`, and input factory.
- `framework/handlers/output`: output controllers (`KafkaOutputController`, `TsdbOutputController`, `ApiOutputController`), factory, and manager.
- `framework/handlers/subscription`: subscription factory and manager for Kafka/MQTT consumers.
- `framework/handlers/utils`: reusable DB, Kafka, and MQTT handlers.
- `framework/utils`: expression evaluator, Redis timeseries singleton, timeseries wrappers.
- `framework/worker`: base worker, deployment helper, state handler, config loader.
- `framework/types.py`: typed contracts for configs, metadata envelopes, and protocols.
- `tests/`: module-level tests and protocol coverage tests.

## Minimal Configuration Example
```text
.
|-- README.md
|-- data_service_sdk/
|-- framework/
|   |-- handlers/
|   |   |-- input/
|   |   |-- output/
|   |   |-- subscription/
|   |   `-- utils/
|   |-- utils/
|   `-- worker/
|       |-- deployment/
|       |-- state_handler/
|       `-- utils/
`-- tests/
```

## Known Constraints and Edge Cases
- Module-local readme files are intentionally removed; all docs live under `docs/`.
- `framework` contains runtime code only; repo-local caches (`.pytest_cache`, `.mypy_cache`, `.ruff_cache`) are non-source artifacts.
- Existing dirty worktree changes outside docs should be treated as unrelated unless explicitly requested.

## Related Links
- [Documentation Index](../index.md)
- [Getting Started Overview](../getting-started/overview.md)
- [Typed Contracts](../reference/types.md)
