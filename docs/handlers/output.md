# Output Handlers

## Purpose
Route produced events to configured destinations (TSDB, Kafka, API, MQTT) using a factory + manager pattern.

## Key Classes and Functions
- `data_service_sdk.handlers.output.factory.Factory.get_output_controller(...)`
- `data_service_sdk.handlers.output.manager.OutputManager`
- `data_service_sdk.handlers.output.tsdb_controller.TsdbOutputController`
- `data_service_sdk.handlers.output.kafka_controller.KafkaOutputController`
- `data_service_sdk.handlers.output.api_controller.ApiOutputController`
- `data_service_sdk.handlers.output.type.OUTPUT_CONTROLLER_TYPE`

## Minimal Configuration Example
```python
from data_service_sdk.handlers.output.manager import OutputManager
from data_service_sdk.handlers.output.type import OUTPUT_CONTROLLER_TYPE

output_handlers_config = [
    {
        "config": {
            "type": OUTPUT_CONTROLLER_TYPE["kafka"],
            "topic": "telemetry.events",
            "bootstrap_servers": "kafka1.example:9092,kafka2.example:9092",
        }
    },
    {
        "config": {
            "type": OUTPUT_CONTROLLER_TYPE["api"],
            "url": "https://api.example/v1/event",
            "url_batch": "https://api.example/v1/event/batch",
            "timeout": 5.0,
        }
    },
]

manager = OutputManager()
manager.init_manager(output_handlers_config)
manager.produce({"id": 1001, "value": 42})
```

TSDB config skeleton:

```python
{
    "type": "2",
    "user": "<db-user>",
    "password": "<db-password>",
    "host": "<db-host>",
    "port": "5432",
    "db": "dataservice",
    "table_name": "timeseries_values",
    "table_schema": [
        {"col_name": "id", "type": "BIGINT", "pkey": True},
        {"col_name": "timestamp", "type": "BIGINT", "pkey": True},
        {"col_name": "value", "type": "DOUBLE_PRECISION"},
    ],
}
```

## Known Constraints and Edge Cases
- Factory expects string type codes (`"1"`, `"2"`, `"3"`, `"4"`) from `OUTPUT_CONTROLLER_TYPE`.
- `KafkaOutputController` expects payloads containing `id` for message key derivation.
- Unknown output type causes `OutputManager.get_controller` to raise `ValueError`.
- TSDB controller retries by resetting table/session on failure.

## Related Links
- [Handler Utilities](utils.md)
- [Typed Contracts](../reference/types.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
