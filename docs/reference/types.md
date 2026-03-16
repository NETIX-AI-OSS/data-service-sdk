# Typed Contracts (`data_service_sdk.types`)

## Purpose
Summarize public typed dictionaries and protocols that define config and message envelope contracts.

## Key Classes and Functions
Input-related:
- `ConditionConfig`, `TransitionConfig`, `StateMachineConfig`, `ExpressionConfig`

Output-related:
- `TsdbTableColumn`, `TsdbOutputConfig`, `KafkaOutputConfig`, `ApiOutputConfig`, `MqttConfig`

Consume envelope-related:
- `MqttConsumeError`, `MqttPacketMetadata`, `MqttConsumedMessage`
- `KafkaConsumeError`, `KafkaHeaderMetadata`, `KafkaPacketMetadata`, `KafkaConsumedMessage`

Protocol interfaces:
- `OutputProducer` (`produce`)
- `OutputProducerFactory` (`__call__`)
- `RowMapping` (`__getitem__`)

Shared aliases:
- `MqttPayloadFormat`, `KafkaPayloadFormat`, `RowMappings`

## Minimal Configuration Example
```python
from data_service_sdk.types import TsdbOutputConfig, KafkaOutputConfig, ApiOutputConfig

tsdb_config: TsdbOutputConfig = {
    "user": "<user>",
    "password": "<password>",
    "host": "<host>",
    "port": "5432",
    "db": "dataservice",
    "table_name": "timeseries_values",
    "table_schema": [
        {"col_name": "id", "type": "BIGINT", "pkey": True},
        {"col_name": "timestamp", "type": "BIGINT", "pkey": True},
        {"col_name": "value", "type": "DOUBLE_PRECISION"},
    ],
}

kafka_config: KafkaOutputConfig = {
    "bootstrap_servers": "kafka1.example:9092",
    "topic": "telemetry.events",
}

api_config: ApiOutputConfig = {
    "url": "https://api.example/event",
    "url_batch": "https://api.example/event/batch",
    "timeout": 5.0,
}
```

## Known Constraints and Edge Cases
- TypedDict contracts are not runtime validation; callers must validate external payloads.
- Protocol methods currently use placeholder method bodies for typing ergonomics and are directly covered by tests.

## Related Links
- [Handler Utilities](../handlers/utils.md)
- [Output Handlers](../handlers/output.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
