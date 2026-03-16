from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List

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


def test_mqtt_handler_consume_json_object(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[int] = []

    def fake_simple(*_args: Any, **_kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            return make_dummy_message(b'{"k": 1}', topic="sensor/a", qos=1, retain=True, mid=12, dup=False)
        raise KeyboardInterrupt()

    monkeypatch.setattr(mqtt_handler.subscribe, "simple", fake_simple)

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

    with pytest.raises(KeyboardInterrupt):
        next(gen)


def test_mqtt_handler_consume_json_primitive(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[int] = []

    def fake_simple(*_args: Any, **_kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            return make_dummy_message(b'"raw-value"', topic="sensor/b")
        raise KeyboardInterrupt()

    monkeypatch.setattr(mqtt_handler.subscribe, "simple", fake_simple)

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
    calls: List[int] = []

    def fake_simple(*_args: Any, **_kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            return make_dummy_message(b"plain-text")
        raise KeyboardInterrupt()

    monkeypatch.setattr(mqtt_handler.subscribe, "simple", fake_simple)

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == b"plain-text"
    assert result["error"] is not None
    assert result["error"]["type"] == "json_decode_error"
    assert result["metadata"]["payload_format"] == "raw_bytes"

    with pytest.raises(KeyboardInterrupt):
        next(gen)


def test_mqtt_handler_consume_non_utf8_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[int] = []

    def fake_simple(*_args: Any, **_kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            return make_dummy_message(b"\xff\xfe\xfd")
        raise KeyboardInterrupt()

    monkeypatch.setattr(mqtt_handler.subscribe, "simple", fake_simple)

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == b"\xff\xfe\xfd"
    assert result["error"] is not None
    assert result["error"]["type"] == "unicode_decode_error"
    assert result["metadata"]["payload_format"] == "raw_bytes"

    with pytest.raises(KeyboardInterrupt):
        next(gen)


def test_mqtt_handler_consume_handles_list_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[int] = []

    def fake_simple(*_args: Any, **_kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            return []
        if len(calls) == 2:
            return [make_dummy_message(b'{"k": 2}', topic="list/topic")]
        raise KeyboardInterrupt()

    monkeypatch.setattr(mqtt_handler.subscribe, "simple", fake_simple)

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == {"k": 2}
    assert result["error"] is None
    assert result["metadata"]["topic"] == "list/topic"

    with pytest.raises(KeyboardInterrupt):
        next(gen)


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


def test_mqtt_handler_consume_handles_type_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[int] = []

    def fake_simple(*_args: Any, **_kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise TypeError("bad payload")
        raise KeyboardInterrupt()

    monkeypatch.setattr(mqtt_handler.subscribe, "simple", fake_simple)

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    with pytest.raises(KeyboardInterrupt):
        next(handler.consume())
