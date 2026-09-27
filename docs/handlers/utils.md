# Handler Utilities

## Purpose
Document reusable low-level integrations for DB writes, Kafka IO, and MQTT IO used by handlers and workers.

## Key Classes and Functions
- `data_service_sdk.handlers.utils.db_handler.TableHandler`
- `data_service_sdk.handlers.utils.db_handler.DataHandler`
- `data_service_sdk.handlers.utils.kafka_handler.KafkaHandler`
- `data_service_sdk.handlers.utils.mqtt_handler.MqttHandler`
- `data_service_sdk.handlers.utils.mqtt_client.create_mqtt_client`
- `data_service_sdk.handlers.utils.mqtt_client.MqttPublisher`

`MqttPublisher` starts an asynchronous network loop and keeps retrying broker
connections by default. It checks queued delivery during `publish()` and
`check_health()` and restarts a stalled network loop after 300 seconds without
discarding Paho's queued QoS 1/2 packets. Call `check_health()` from an idle
worker's regular poll loop. Set `MQTT_GIVE_UP_SECS` to a finite number of seconds
to restore the previous fail-after-deadline behavior; `0` means fail on the
first error. Explicit `inf` means retry indefinitely. The older
`MQTT_PUBLISH_GIVE_UP_SECS` and `MQTT_RECONNECT_GIVE_UP_SECS` names remain
supported, with deprecation warnings. The SDK's MQTT consumer uses the same
setting.

The outgoing queue is capped at 4,096 QoS 1/2 packets. `publish()` returns
`None` when it cannot confirm acceptance (for example, QoS 0 while offline,
a full queue, or a socket exception); the caller must retry or persist that
payload. A socket exception can occur after enqueueing, so retries can produce
duplicates and consumers should handle them idempotently. A returned
`MQTTMessageInfo` means Paho accepted the packet, not that the broker
acknowledged it. An offline QoS 1/2 packet can return an info object with
`rc=MQTT_ERR_NO_CONN` while remaining in Paho's queue. Its
`is_published()`/`wait_for_publish()` methods may raise because of that original
return code, even after delivery; `check_health()` tracks it separately. Delivery
across process restarts requires a durable upstream queue; QoS 0 can be lost
during disconnection. `stop()` cancels recovery and disconnects the client.

## Minimal Configuration Example
DB handlers:

```python
from data_service_sdk.handlers.utils.db_handler import TableHandler, DataHandler

table = TableHandler("<user>", "<password>", "<host>", "5432", "dataservice")
_ = table.make_table_if_not_exists(
    "timeseries_values",
    [
        {"col_name": "id", "type": "BIGINT", "pkey": True},
        {"col_name": "timestamp", "type": "BIGINT", "pkey": True},
        {"col_name": "value", "type": "DOUBLE_PRECISION"},
    ],
)

data = DataHandler("<user>", "<password>", "<host>", "5432", "dataservice")
data.set_table("timeseries_values")
data.insert_tsdb([{"id": 1, "timestamp": 1700000000000, "value": 9.2}])
```

Kafka consume envelope (typed by `KafkaConsumedMessage`):

```python
{
    "data": {"k": "v"} | b"raw" | None,
    "metadata": {
        "topic": "...",
        "subscription_topic": "...",
        "partition": 0,
        "offset": 10,
        "timestamp": 1700000000000,
        "timestamp_type": 1,
        "key_text": "device-1",
        "key_bytes_b64": "...",
        "headers": [{"key": "h", "value_text": "v", "value_bytes_b64": "..."}],
        "payload_size_bytes": 12,
        "received_at_ms": 1700000000001,
        "received_at_iso": "2026-01-01T00:00:00+00:00",
        "payload_format": "json" | "raw_bytes",
    },
    "error": {"type": "json_decode_error", "message": "..."} | None,
}
```

MQTT consume envelope (typed by `MqttConsumedMessage`) mirrors the same pattern with MQTT-specific metadata fields.

## Known Constraints and Edge Cases
- DB `insert_tsdb` falls back to linear inserts on `IntegrityError` and resets session on generic failures.
- Kafka/MQTT payload parsing attempts JSON first; non-JSON and decode failures are preserved as raw bytes with error metadata.
- Kafka/MQTT producer/consumer usage requires explicit initialization, otherwise runtime errors are raised.
- Kafka metadata includes base64 key/header preservation for non-UTF8 payloads.

## Related Links
- [Output Handlers](output.md)
- [Subscription Handlers](subscription.md)
- [Typed Contracts](../reference/types.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
