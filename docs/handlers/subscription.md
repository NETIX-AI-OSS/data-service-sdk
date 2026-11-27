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

MQTT consumers use plaintext TCP/1883 by default for compatibility. Set
`MQTT_TLS=true` to require server certificate verification; this defaults the
port to 8883. `MQTT_PORT` can override the port, and `MQTT_CA_FILE` can point to
a mounted CA file when the broker uses a private CA. Use a DNS name present in
the server certificate's SAN. Invalid transport settings fail before the
consumer connects. `MQTT_CA_FILE` without `MQTT_TLS=true` is rejected to avoid
silently sending credentials over plaintext.

## Known Constraints and Edge Cases
- Factory uses string type values (`"1"` for Kafka, `"2"` for MQTT).
- Unknown type returns `None`; caller should validate before use.
- Factory includes fallback defaults for missing config keys; explicit configs are preferred in production.
- Kafka creates one `KafkaConsumer` during `init_consumer(...)` and reuses it for the consume loop.
- MQTT uses one long-lived client per handler instance and only subscribes when a new broker session is established.

## Related Links
- [Handler Utilities](utils.md)
- [Typed Contracts](../reference/types.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
