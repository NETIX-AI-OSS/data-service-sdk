from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List, cast

import pytest

from framework.handlers.utils import mqtt_handler


def make_dummy_message(payload: bytes, **overrides: Any) -> Any:
    message_data = {
        "payload": payload,
        "topic": "topic",
        "qos": 0,
        "retain": False,
        "mid": 1,
        "dup": False,
    }
    message_data.update(overrides)
    return SimpleNamespace(**message_data)


class DummyClient:
    def __init__(self, client_id: str = "") -> None:
        self.client_id = client_id
        self.call_history: dict[str, Any] = {
            "connect_calls": [],
            "subscribe_calls": [],
            "reconnect_calls": 0,
            "disconnect_calls": 0,
            "loop_count": 0,
        }
        self.results: dict[str, Any] = {
            "connect": mqtt_handler.mqtt.MQTT_ERR_SUCCESS,
            "reconnect": mqtt_handler.mqtt.MQTT_ERR_SUCCESS,
            "subscribe": mqtt_handler.mqtt.MQTT_ERR_SUCCESS,
        }
        self.config: dict[str, Any] = {
            "username_pw": None,
            "logger": None,
        }
        self.events: list[tuple[str, Any]] = []
        self.callbacks: dict[str, Any] = {
            "on_connect": None,
            "on_disconnect": None,
            "on_message": None,
        }

    @property
    def on_connect(self) -> Any:
        return self.callbacks["on_connect"]

    @on_connect.setter
    def on_connect(self, value: Any) -> None:
        self.callbacks["on_connect"] = value

    @property
    def on_disconnect(self) -> Any:
        return self.callbacks["on_disconnect"]

    @on_disconnect.setter
    def on_disconnect(self, value: Any) -> None:
        self.callbacks["on_disconnect"] = value

    @property
    def on_message(self) -> Any:
        return self.callbacks["on_message"]

    @on_message.setter
    def on_message(self, value: Any) -> None:
        self.callbacks["on_message"] = value

    def enable_logger(self, logger: Any = None) -> None:
        self.config["logger"] = logger

    def username_pw_set(self, username: str | None, password: str | None = None) -> None:
        self.config["username_pw"] = (username, password)

    def reconnect_delay_set(self, min_delay: int = 1, max_delay: int = 120) -> None:
        _ = (min_delay, max_delay)

    def connect(self, host: str, port: int = 1883, keepalive: int = 60) -> int:
        self.call_history["connect_calls"].append((host, port, keepalive))
        return cast(int, self.results["connect"])

    def reconnect(self) -> int:
        self.call_history["reconnect_calls"] += 1
        return cast(int, self.results["reconnect"])

    def subscribe(self, topic: str, qos: int = 0) -> tuple[int, int]:
        self.call_history["subscribe_calls"].append((topic, qos))
        return cast(int, self.results["subscribe"]), len(self.call_history["subscribe_calls"])

    def loop(self, timeout: float = 1.0) -> int:
        _ = timeout
        self.call_history["loop_count"] += 1
        if not self.events:
            raise KeyboardInterrupt()

        event_name, event_value = self.events.pop(0)
        if event_name == "connect":
            callback = self.on_connect
            if callable(callback):
                callback(self, None, SimpleNamespace(session_present=event_value), 0, None)
            return mqtt_handler.mqtt.MQTT_ERR_SUCCESS
        if event_name == "message":
            callback = self.on_message
            if callable(callback):
                callback(self, None, event_value)
            return mqtt_handler.mqtt.MQTT_ERR_SUCCESS
        if event_name == "disconnect":
            callback = self.on_disconnect
            if callable(callback):
                callback(self, None, None, event_value, None)
            return mqtt_handler.mqtt.MQTT_ERR_SUCCESS
        if event_name == "loop_error":
            return cast(int, event_value)
        raise AssertionError(f"Unknown event: {event_name}")

    def disconnect(self, *_args: Any, **_kwargs: Any) -> int:
        self.call_history["disconnect_calls"] += 1
        return mqtt_handler.mqtt.MQTT_ERR_SUCCESS


def install_dummy_client(monkeypatch: pytest.MonkeyPatch, events: list[tuple[str, Any]]) -> DummyClient:
    client = DummyClient()
    client.events = list(events)

    def fake_client(*args: Any, **kwargs: Any) -> DummyClient:
        _ = args
        if "client_id" in kwargs:
            client.client_id = kwargs["client_id"]
        return client

    monkeypatch.setattr(mqtt_handler.mqtt, "Client", fake_client)
    return client


def build_consumer_state(**kwargs: Any) -> Any:
    return getattr(mqtt_handler, "_MqttConsumerState")(**kwargs)


def invoke_handler_method(handler: mqtt_handler.MqttHandler, name: str, *args: Any) -> Any:
    return getattr(handler, name)(*args)


def set_handler_attr(handler: mqtt_handler.MqttHandler, name: str, value: Any) -> None:
    setattr(handler, name, value)


def get_handler_attr(handler: mqtt_handler.MqttHandler, name: str) -> Any:
    return getattr(handler, name)


def test_mqtt_handler_consume_json_object(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'{"k": 1}', topic="sensor/a", qos=1, retain=True, mid=12, dup=False)),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == {"k": 1}
    assert result["error"] is None
    metadata = result["metadata"]
    assert metadata["topic"] == "sensor/a"
    assert metadata["subscription_topic"] == "t"
    assert metadata["host"] == "h"
    assert metadata["qos"] == 1
    assert metadata["retain"] is True
    assert metadata["mid"] == 12
    assert metadata["dup"] is False
    assert metadata["payload_size_bytes"] == len(b'{"k": 1}')
    assert metadata["payload_format"] == "json"
    assert isinstance(metadata["received_at_ms"], int)
    assert isinstance(metadata["received_at_iso"], str)
    assert client.call_history["connect_calls"] == [("h", 1883, 25)]
    assert client.call_history["subscribe_calls"] == [("t", 0)]
    assert client.config["username_pw"] == ("u", "p")
    assert client.client_id.startswith("dsdk-")


def test_mqtt_handler_consume_json_primitive(monkeypatch: pytest.MonkeyPatch) -> None:
    install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'"raw-value"', topic="sensor/b")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == "raw-value"
    assert result["error"] is None
    assert result["metadata"]["payload_format"] == "json"

    with pytest.raises(KeyboardInterrupt):
        next(gen)


def test_mqtt_handler_consume_utf8_non_json(monkeypatch: pytest.MonkeyPatch) -> None:
    install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b"plain-text")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == b"plain-text"
    assert result["error"] is not None
    assert result["error"]["type"] == "json_decode_error"
    assert result["metadata"]["payload_format"] == "raw_bytes"


def test_mqtt_handler_consume_non_utf8_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b"\xff\xfe\xfd")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == b"\xff\xfe\xfd"
    assert result["error"] is not None
    assert result["error"]["type"] == "unicode_decode_error"
    assert result["metadata"]["payload_format"] == "raw_bytes"


def test_mqtt_handler_consume_subscribes_once_for_multiple_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'{"k": 1}', topic="sensor/1")),
            ("message", make_dummy_message(b'{"k": 2}', topic="sensor/2")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    first = next(gen)
    second = next(gen)
    assert first["data"] == {"k": 1}
    assert second["data"] == {"k": 2}
    assert client.call_history["connect_calls"] == [("h", 1883, 25)]
    assert client.call_history["subscribe_calls"] == [("t", 0)]
    assert client.call_history["reconnect_calls"] == 0


def test_mqtt_handler_reuses_existing_client_across_new_consume_generators(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'{"k": 1}', topic="sensor/1")),
            ("message", make_dummy_message(b'{"k": 2}', topic="sensor/2")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    first = next(handler.consume())
    second = next(handler.consume())
    assert first["data"] == {"k": 1}
    assert second["data"] == {"k": 2}
    assert client.call_history["connect_calls"] == [("h", 1883, 25)]
    assert client.call_history["subscribe_calls"] == [("t", 0)]


def test_mqtt_handler_consume_resumes_existing_session_without_resubscribe(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", True),
            ("message", make_dummy_message(b'{"k": 3}', topic="sensor/resume")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    result = next(handler.consume())
    assert result["data"] == {"k": 3}
    assert not client.call_history["subscribe_calls"]


def test_mqtt_handler_produce_and_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[Any] = []

    def fake_single(*_args: Any, **_kwargs: Any) -> None:
        calls.append(("ok", _args, _kwargs))

    monkeypatch.setattr(mqtt_handler.publish, "single", fake_single)

    handler = mqtt_handler.MqttHandler()
    handler.init_producer("t", "h", "u", "p")
    handler.produce({"k": 1})

    assert calls

    def raise_single(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("fail")

    monkeypatch.setattr(mqtt_handler.publish, "single", raise_single)
    handler.produce({"k": 2})


def test_mqtt_handler_flush_is_noop() -> None:
    handler = mqtt_handler.MqttHandler()
    handler.flush()


def test_mqtt_handler_requires_init() -> None:
    handler = mqtt_handler.MqttHandler()

    with pytest.raises(RuntimeError):
        next(handler.consume())

    with pytest.raises(RuntimeError):
        handler.produce({"k": 1})


def test_mqtt_handler_normalize_payload_variants() -> None:
    normalize_payload = getattr(mqtt_handler.MqttHandler, "_normalize_payload")
    assert normalize_payload(bytearray(b"a")) == b"a"
    assert normalize_payload(None) == b""
    assert normalize_payload(99) == b"99"


def test_mqtt_handler_consume_reconnects_without_resubscribing_existing_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("disconnect", 1),
            ("connect", True),
            ("message", make_dummy_message(b'{"k": 4}', topic="sensor/reconnect")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    result = next(handler.consume())
    assert result["data"] == {"k": 4}
    assert client.call_history["subscribe_calls"] == [("t", 0)]
    assert client.call_history["reconnect_calls"] == 1


def test_mqtt_handler_close_consumer_disconnects_client(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [("connect", False)])

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")
    with pytest.raises(KeyboardInterrupt):
        next(handler.consume())

    handler.close_consumer()
    assert client.call_history["disconnect_calls"] == 1


def test_mqtt_handler_on_connect_sets_error_for_nonzero_reason_code() -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id"),
    )
    client = DummyClient()

    invoke_handler_method(
        handler, "_on_connect", cast(Any, client), None, cast(Any, SimpleNamespace(session_present=False)), 5, None
    )

    error = get_handler_attr(handler, "_MqttHandler__consumer_state").connect_error
    assert error is not None
    assert "failed: 5" in str(error)


def test_mqtt_handler_on_connect_without_state_is_noop() -> None:
    handler = mqtt_handler.MqttHandler()
    invoke_handler_method(
        handler, "_on_connect", cast(Any, DummyClient()), None, cast(Any, SimpleNamespace(session_present=False)), 0, None
    )


def test_mqtt_handler_on_connect_requires_topic_for_new_session() -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id"),
    )
    client = DummyClient()

    invoke_handler_method(
        handler, "_on_connect", cast(Any, client), None, cast(Any, SimpleNamespace(session_present=False)), 0, None
    )

    error = get_handler_attr(handler, "_MqttHandler__consumer_state").connect_error
    assert error is not None
    assert str(error) == "MQTT consumer topic not configured"


def test_mqtt_handler_on_connect_sets_error_when_subscribe_fails() -> None:
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")
    client = DummyClient()
    client.results["subscribe"] = 7

    invoke_handler_method(
        handler, "_on_connect", cast(Any, client), None, cast(Any, SimpleNamespace(session_present=False)), 0, None
    )

    error = get_handler_attr(handler, "_MqttHandler__consumer_state").connect_error
    assert error is not None
    assert "subscribe failed" in str(error)


def test_mqtt_handler_on_disconnect_handles_clean_disconnect() -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id", connected=True),
    )

    invoke_handler_method(handler, "_on_disconnect", cast(Any, DummyClient()), None, None, 0, None)
    assert get_handler_attr(handler, "_MqttHandler__consumer_state").connected is False

    get_handler_attr(handler, "_MqttHandler__consumer_state").connected = True
    invoke_handler_method(handler, "_on_disconnect", cast(Any, DummyClient()), None, None, None, None)
    assert get_handler_attr(handler, "_MqttHandler__consumer_state").connected is False


def test_mqtt_handler_on_disconnect_without_state_is_noop() -> None:
    invoke_handler_method(mqtt_handler.MqttHandler(), "_on_disconnect", cast(Any, DummyClient()), None, None, 0, None)


def test_mqtt_handler_on_message_without_queue_is_noop() -> None:
    handler = mqtt_handler.MqttHandler()
    invoke_handler_method(handler, "_on_message", cast(Any, DummyClient()), None, make_dummy_message(b"ignored"))


def test_mqtt_handler_ensure_consumer_client_connect_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [])
    client.results["connect"] = 4

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    with pytest.raises(RuntimeError, match="connect failed"):
        invoke_handler_method(handler, "_ensure_consumer_client")


def test_mqtt_handler_wait_for_connection_raises_stored_error() -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id", connect_error=RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        invoke_handler_method(handler, "_wait_for_connection", cast(Any, DummyClient()))

    assert get_handler_attr(handler, "_MqttHandler__consumer_state").connect_error is None


def test_mqtt_handler_wait_for_connection_requires_state() -> None:
    with pytest.raises(RuntimeError, match="not initialized"):
        invoke_handler_method(mqtt_handler.MqttHandler(), "_wait_for_connection", cast(Any, DummyClient()))


def test_mqtt_handler_wait_for_connection_raises_on_loop_error() -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id"),
    )
    client = DummyClient()
    client.events = [("loop_error", 6)]

    with pytest.raises(RuntimeError, match="loop failed during connect"):
        invoke_handler_method(handler, "_wait_for_connection", cast(Any, client))


def test_mqtt_handler_get_next_message_requires_initialized_consumer() -> None:
    with pytest.raises(RuntimeError, match="not initialized"):
        invoke_handler_method(mqtt_handler.MqttHandler(), "_get_next_message")


def test_mqtt_handler_get_next_message_requires_state_after_client_init(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = mqtt_handler.MqttHandler()

    def _dummy_client() -> DummyClient:
        return DummyClient()

    monkeypatch.setattr(handler, "_ensure_consumer_client", _dummy_client)

    with pytest.raises(RuntimeError, match="not initialized"):
        invoke_handler_method(handler, "_get_next_message")


def test_mqtt_handler_get_next_message_requires_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id"),
    )
    get_handler_attr(handler, "_MqttHandler__consumer_state").messages = None

    def _dummy_client() -> DummyClient:
        return DummyClient()

    monkeypatch.setattr(handler, "_ensure_consumer_client", _dummy_client)

    with pytest.raises(RuntimeError, match="message queue not initialized"):
        invoke_handler_method(handler, "_get_next_message")


def test_mqtt_handler_get_next_message_raises_stored_connect_error(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(
            client_id="client-id",
            messages=mqtt_handler.queue.Queue(),
            connected=True,
            connect_error=RuntimeError("connect boom"),
        ),
    )

    def _dummy_client() -> DummyClient:
        return DummyClient()

    monkeypatch.setattr(handler, "_ensure_consumer_client", _dummy_client)

    with pytest.raises(RuntimeError, match="connect boom"):
        invoke_handler_method(handler, "_get_next_message")

    assert get_handler_attr(handler, "_MqttHandler__consumer_state").connect_error is None


def test_mqtt_handler_get_next_message_raises_on_reconnect_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = mqtt_handler.MqttHandler()
    client = DummyClient()
    client.results["reconnect"] = 9
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id", messages=mqtt_handler.queue.Queue(), connected=False),
    )

    def _dummy_client() -> DummyClient:
        return client

    monkeypatch.setattr(handler, "_ensure_consumer_client", _dummy_client)

    with pytest.raises(RuntimeError, match="reconnect failed"):
        invoke_handler_method(handler, "_get_next_message")


def test_mqtt_handler_close_consumer_clears_state_without_client() -> None:
    handler = mqtt_handler.MqttHandler()
    set_handler_attr(handler, "_MqttHandler__consumer_state", build_consumer_state(client_id="client-id"))

    handler.close_consumer()
    assert get_handler_attr(handler, "_MqttHandler__consumer_state") is None


def test_mqtt_handler_consume_recovers_after_loop_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("loop_error", 4),
            ("connect", True),
            ("message", make_dummy_message(b'{"k": 5}', topic="sensor/loop-error")),
        ],
    )

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    result = next(handler.consume())
    assert result["data"] == {"k": 5}
    assert client.call_history["reconnect_calls"] == 1
