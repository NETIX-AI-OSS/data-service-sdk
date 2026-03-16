# Subscription Handlers

## Purpose
Build inbound subscription consumers for Kafka and MQTT using type-driven factory selection.

## Key Classes and Functions
- `data_service_sdk.handlers.subscription.factory.Factory.get_subscription_controller(...)`
- `data_service_sdk.handlers.subscription.manager.SubscriptionManager.get_controller(...)`
- `data_service_sdk.handlers.subscription.type.SUBSCRIPTION_TYPE`

## Minimal Configuration Example
Kafka subscription:

```python
from data_service_sdk.handlers.subscription.factory import Factory
from data_service_sdk.handlers.subscription.type import SUBSCRIPTION_TYPE

controller = Factory.get_subscription_controller(
    SUBSCRIPTION_TYPE["kafka"],
    {
        "kafka_topic_subscribe": "telemetry.events",
        "kafka_bootstrap_servers": "kafka1.example:9092,kafka2.example:9092",
        "kafka_ts_group_id": "data-worker",
        "kafka_ts_offset_reset": "earliest",
        "kafka_ts_auto_commit": True,
        "polling_interval_secs": 5.0,
    },
)
```

MQTT subscription:

```python
controller = Factory.get_subscription_controller(
    SUBSCRIPTION_TYPE["mqtt"],
    {
        "topic": "telemetry/#",
        "host": "mqtt.example",
        "user": "<mqtt-user>",
        "password": "<mqtt-password>",
    },
)
```

## Known Constraints and Edge Cases
- Factory uses string type values (`"1"` for Kafka, `"2"` for MQTT).
- Unknown type returns `None`; caller should validate before use.
- Factory includes fallback defaults for missing config keys; explicit configs are preferred in production.

## Related Links
- [Handler Utilities](utils.md)
- [Typed Contracts](../reference/types.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
