from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any, Iterator, List, cast

import pytest

from framework.handlers.utils import kafka_handler


def make_dummy_message(offset: int, value: Any = None, **overrides: Any) -> Any:
    message_data: dict[str, Any] = {
        "offset": offset,
        "value": b'{"offset": 1}' if value is None else value,
        "topic": "topic",
        "partition": 0,
        "timestamp": 1000,
        "timestamp_type": 1,
        "key": None,
        "headers": [],
    }
    message_data.update(overrides)
    if message_data["headers"] is None:
        message_data["headers"] = []
    return SimpleNamespace(**message_data)


class DummyConsumer:
    def __init__(self, messages: List[Any]) -> None:
        self._messages = messages
        self.assigned: List[Any] = []
        self.seek_calls: List[Any] = []

    def assign(self, partitions: list[Any]) -> None:
        self.assigned = partitions

    def seek(self, partition: Any, offset: int) -> None:
        self.seek_calls.append((partition, offset))

    def __iter__(self) -> Iterator[Any]:
        return iter(self._messages)


class DummyProducer:
    def __init__(self) -> None:
        self.sent: List[Any] = []
        self.flushed = False

    def send(self, topic: str, key: bytes, value: Any) -> str:
        self.sent.append((topic, key, value))
        return "sent"

    def flush(self) -> str:
        self.flushed = True
        return "flushed"


def test_kafka_handler_assign_and_seek(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = DummyConsumer(
        [
            make_dummy_message(1, value=b'{"offset": 1}', topic="a", partition=0, key=b"key1", headers=[("h1", b"v1")]),
            make_dummy_message(2, value=b'{"offset": 2}', topic="a", partition=0),
            make_dummy_message(3, value=b'{"offset": 3}', topic="a", partition=0),
        ]
    )

    def fake_consumer(*_args: Any, **_kwargs: Any) -> DummyConsumer:
        return consumer

    monkeypatch.setattr(kafka_handler, "KafkaConsumer", fake_consumer)

    handler = kafka_handler.KafkaHandler()
    results = list(handler.assign_and_seek(start=1, end=2, topic="t", kafka_bootstrap_servers="b"))

    assert [msg["metadata"]["offset"] for msg in results] == [1, 2]
    assert results[0]["data"] == {"offset": 1}
    assert results[0]["error"] is None
    assert results[0]["metadata"]["topic"] == "a"
    assert results[0]["metadata"]["subscription_topic"] == "t"
    assert results[0]["metadata"]["partition"] == 0
    assert results[0]["metadata"]["payload_format"] == "json"
    assert results[0]["metadata"]["key_text"] == "key1"
    assert results[0]["metadata"]["key_bytes_b64"] == base64.b64encode(b"key1").decode("ascii")
    assert results[0]["metadata"]["headers"][0]["key"] == "h1"
    assert results[0]["metadata"]["headers"][0]["value_text"] == "v1"
    assert results[0]["metadata"]["headers"][0]["value_bytes_b64"] == base64.b64encode(b"v1").decode("ascii")


def test_kafka_handler_assign_and_seek_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = DummyConsumer([])

    def fake_consumer(*_args: Any, **_kwargs: Any) -> DummyConsumer:
        return consumer

    monkeypatch.setattr(kafka_handler, "KafkaConsumer", fake_consumer)

    handler = kafka_handler.KafkaHandler()
    results = list(handler.assign_and_seek(start=1, end=2, topic="t", kafka_bootstrap_servers="b"))

    assert not results


def test_kafka_handler_assign_and_seek_non_json_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = DummyConsumer(
        [make_dummy_message(1, value=b"not-json", topic="raw-topic", key=b"\xff", headers=[("h", b"\xff")])]
    )

    def fake_consumer(*_args: Any, **_kwargs: Any) -> DummyConsumer:
        return consumer

    monkeypatch.setattr(kafka_handler, "KafkaConsumer", fake_consumer)

    handler = kafka_handler.KafkaHandler()
    results = list(handler.assign_and_seek(start=1, end=1, topic="t", kafka_bootstrap_servers="b"))

    assert len(results) == 1
    result = results[0]
    assert result["data"] == b"not-json"
    assert result["error"] is not None
    assert result["error"]["type"] == "json_decode_error"
    assert result["metadata"]["payload_format"] == "raw_bytes"
    assert result["metadata"]["key_text"] is None
    assert result["metadata"]["key_bytes_b64"] == base64.b64encode(b"\xff").decode("ascii")
    assert result["metadata"]["headers"][0]["value_text"] is None
    assert result["metadata"]["headers"][0]["value_bytes_b64"] == base64.b64encode(b"\xff").decode("ascii")


def test_kafka_handler_assign_and_seek_empty_key(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = DummyConsumer([make_dummy_message(1, value=b'{"v": 1}', key=b"")])

    def fake_consumer(*_args: Any, **_kwargs: Any) -> DummyConsumer:
        return consumer

    monkeypatch.setattr(kafka_handler, "KafkaConsumer", fake_consumer)

    handler = kafka_handler.KafkaHandler()
    results = list(handler.assign_and_seek(start=1, end=1, topic="t", kafka_bootstrap_servers="b"))

    assert len(results) == 1
    result = results[0]
    assert result["error"] is None
    assert result["metadata"]["key_text"] == ""
    assert result["metadata"]["key_bytes_b64"] == ""


def test_kafka_handler_produce_and_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = DummyProducer()

    def fake_producer(*_args: Any, **_kwargs: Any) -> DummyProducer:
        return producer

    monkeypatch.setattr(kafka_handler, "KafkaProducer", fake_producer)

    handler = kafka_handler.KafkaHandler()

    with pytest.raises(RuntimeError):
        handler.produce("k", {"v": 1})

    handler.init_producer(topic_name="t", bootstrap_servers="b")
    result = handler.produce("k", {"v": 1})
    handler.flush()

    assert result == "sent"
    assert producer.flushed is True
    assert producer.sent == [("t", b"k", {"v": 1})]


def test_kafka_handler_consume_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = DummyConsumer([make_dummy_message(1, value=b'{"offset": 1}', topic="loop-topic")])

    def fake_consumer(*_args: Any, **_kwargs: Any) -> DummyConsumer:
        return consumer

    monkeypatch.setattr(kafka_handler, "KafkaConsumer", fake_consumer)

    handler = kafka_handler.KafkaHandler()
    handler.init_consumer(
        topic="t",
        kafka_bootstrap_servers="b",
        kafka_ts_group_id="g",
        kafka_ts_offset_reset="earliest",
        kafka_ts_auto_commit=True,
        polling_interval_secs=0.0,
    )

    def fake_sleep(_seconds: float) -> None:
        raise RuntimeError("stop")

    monkeypatch.setattr(kafka_handler.time, "sleep", fake_sleep)

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == {"offset": 1}
    assert result["error"] is None
    assert result["metadata"]["topic"] == "loop-topic"

    with pytest.raises(RuntimeError, match="stop"):
        next(gen)


def test_kafka_handler_consume_reuses_single_consumer_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = DummyConsumer(
        [
            make_dummy_message(1, value=b'{"offset": 1}', topic="loop-topic"),
            make_dummy_message(2, value=b'{"offset": 2}', topic="loop-topic"),
        ]
    )
    calls: List[tuple[Any, Any]] = []

    def fake_consumer(*args: Any, **kwargs: Any) -> DummyConsumer:
        calls.append((args, kwargs))
        return consumer

    monkeypatch.setattr(kafka_handler, "KafkaConsumer", fake_consumer)

    handler = kafka_handler.KafkaHandler()
    handler.init_consumer(
        topic="t",
        kafka_bootstrap_servers="b",
        kafka_ts_group_id="g",
        kafka_ts_offset_reset="earliest",
        kafka_ts_auto_commit=True,
        polling_interval_secs=0.0,
    )

    gen = handler.consume()
    first = next(gen)
    second = next(gen)
    assert first["data"] == {"offset": 1}
    assert second["data"] == {"offset": 2}
    assert len(calls) == 1


def test_kafka_handler_consume_requires_init() -> None:
    handler = kafka_handler.KafkaHandler()

    with pytest.raises(RuntimeError):
        next(handler.consume())


def test_kafka_handler_flush_requires_init() -> None:
    handler = kafka_handler.KafkaHandler()

    with pytest.raises(RuntimeError):
        handler.flush()


def test_kafka_handler_normalize_payload_variants() -> None:
    normalize_payload = getattr(kafka_handler.KafkaHandler, "_normalize_payload")
    assert normalize_payload(bytearray(b"abc")) == b"abc"
    assert normalize_payload(None) == b""
    assert normalize_payload("txt") == b"txt"
    assert normalize_payload(123) == b"123"


def test_kafka_handler_extract_key_and_headers_non_list_headers() -> None:
    message = make_dummy_message(
        1,
        key=bytearray(b"k"),
        headers=[("ignore", b"v")],
    )
    message.headers = cast(Any, ("not", "a", "list"))

    extract_key_and_headers = getattr(kafka_handler.KafkaHandler, "_extract_key_and_headers")
    key_text, key_b64, headers = extract_key_and_headers(message)
    assert key_text == "k"
    assert key_b64 == base64.b64encode(b"k").decode("ascii")
    assert not headers


def test_kafka_handler_extract_key_and_headers_skips_invalid_and_handles_none() -> None:
    message = make_dummy_message(1, key=b"s-key")
    message.key = cast(Any, "s-key")
    message.headers = cast(Any, [("h1", None), ("bad",), "bad"])

    extract_key_and_headers = getattr(kafka_handler.KafkaHandler, "_extract_key_and_headers")
    key_text, key_b64, headers = extract_key_and_headers(message)
    assert key_text == "s-key"
    assert key_b64 == base64.b64encode(b"s-key").decode("ascii")
    assert len(headers) == 1
    assert headers[0]["key"] == "h1"
    assert headers[0]["value_text"] is None
    assert headers[0]["value_bytes_b64"] is None


def test_kafka_handler_parse_payload_unicode_decode_error() -> None:
    parse_payload = getattr(kafka_handler.KafkaHandler, "_parse_payload")
    payload, error, payload_format = parse_payload(b"\x80")
    assert payload == b"\x80"
    assert error is not None
    assert error["type"] == "unicode_decode_error"
    assert payload_format == "raw_bytes"


def test_kafka_handler_consume_loop_logs_parse_error(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = DummyConsumer([make_dummy_message(1, value=b"not-json", topic="loop-topic")])

    def fake_consumer(*_args: Any, **_kwargs: Any) -> DummyConsumer:
        return consumer

    monkeypatch.setattr(kafka_handler, "KafkaConsumer", fake_consumer)

    handler = kafka_handler.KafkaHandler()
    handler.init_consumer(
        topic="t",
        kafka_bootstrap_servers="b",
        kafka_ts_group_id="g",
        kafka_ts_offset_reset="earliest",
        kafka_ts_auto_commit=True,
        polling_interval_secs=0.0,
    )

    def fake_sleep(_seconds: float) -> None:
        raise RuntimeError("stop")

    monkeypatch.setattr(kafka_handler.time, "sleep", fake_sleep)

    gen = handler.consume()
    result = next(gen)
    assert result["data"] == b"not-json"
    assert result["error"] is not None
    assert result["error"]["type"] == "json_decode_error"
