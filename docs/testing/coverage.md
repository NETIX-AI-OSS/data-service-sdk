# Test Coverage Map

## Purpose
Map test files to runtime modules and critical behavior so contributors know where regressions are guarded.

## Key Classes and Functions
- Entry command: `uv run pytest`
- Fixtures and dependency shims: `tests/conftest.py`

## Minimal Configuration Example
```bash
# Run all tests
uv run pytest

# Optional focused runs
uv run pytest tests/test_kafka_handler.py
uv run pytest tests/test_mqtt_handler.py
uv run pytest tests/test_protocol_coverage.py
```

## Known Constraints and Edge Cases
- Tests rely heavily on monkeypatch/mocks for Kafka, MQTT, Redis, Kubernetes, and SQLAlchemy behavior.
- Runtime integrations are validated at unit-contract level, not end-to-end infra level.

## Module-to-Test Mapping
- `framework.handlers.input.*`
  - `tests/test_input_factory_and_controller.py`
  - `tests/test_expression_and_state_machine.py`
- `framework.handlers.output.*`
  - `tests/test_output_controllers.py`
  - `tests/test_output_factory_and_manager.py`
- `framework.handlers.subscription.*`
  - `tests/test_subscription_factory_and_manager.py`
- `framework.handlers.utils.db_handler`
  - `tests/test_db_handler.py`
- `framework.handlers.utils.kafka_handler`
  - `tests/test_kafka_handler.py`
- `framework.handlers.utils.mqtt_handler`
  - `tests/test_mqtt_handler.py`
- `framework.utils.timeseries*`
  - `tests/test_timeseries.py`
  - `tests/test_timeseries_manager.py`
  - `tests/test_redis_timeseries.py`
- `framework.worker.*`
  - `tests/test_worker_utils.py`
- Protocol method/body coverage
  - `tests/test_protocol_coverage.py`

## Related Links
- [Documentation Index](../index.md)
- [Typed Contracts](../reference/types.md)
- [Worker Deployment](../worker/deployment.md)
