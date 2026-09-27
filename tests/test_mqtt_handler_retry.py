from __future__ import annotations

from typing import Any, cast

import pytest

from test_mqtt_handler import (
    DummyClient,
    build_consumer_state,
    get_handler_attr,
    install_dummy_client,
    invoke_handler_method,
    make_dummy_message,
    record_retry_waits,
    set_handler_attr,
)
from framework.handlers.utils import mqtt_handler


def test_mqtt_handler_ensure_consumer_client_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MQTT_GIVE_UP_SECS", "0")
    client = install_dummy_client(monkeypatch, [])
    client.results["connect"] = 4

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("t", "h", "u", "p")

    with pytest.raises(RuntimeError, match="could not connect within 0s"):
        invoke_handler_method(handler, "_ensure_consumer_client")
    assert client.call_history["connect_calls"] == [("h", 1883, 25)]


def test_mqtt_handler_zero_deadline_allows_immediate_handshake(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [("connect", False), ("message", make_dummy_message(b"42"))])
    monkeypatch.setenv("MQTT_GIVE_UP_SECS", "0")
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")

    assert next(handler.consume())["data"] == 42
    assert client.call_history["connect_calls"] == [("host", 1883, 25)]


def test_mqtt_handler_finite_deadline_bounds_silent_initial_handshake(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [])
    polls = []

    def silent_loop(timeout: float = 1.0) -> int:
        polls.append(timeout)
        return mqtt_handler.mqtt.MQTT_ERR_SUCCESS

    client.loop = silent_loop  # type: ignore[method-assign]
    monkeypatch.setenv("MQTT_GIVE_UP_SECS", "0")
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")

    with pytest.raises(RuntimeError, match="could not connect within 0s"):
        next(handler.consume())
    assert client.call_history["connect_calls"] == [("host", 1883, 25)]
    assert polls == [1.0]


def test_mqtt_handler_initial_connection_retries_dns_failure_without_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(monkeypatch, [("connect", False), ("message", make_dummy_message(b"42"))])
    connect = client.connect
    failures = iter([OSError("DNS unavailable"), OSError("DNS unavailable")])

    def flaky_connect(*args: Any, **kwargs: Any) -> int:
        failure = next(failures, None)
        if failure is not None:
            raise failure
        return connect(*args, **kwargs)

    client.connect = flaky_connect  # type: ignore[method-assign]
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
    waits = record_retry_waits(handler)
    monkeypatch.setattr(handler, "_reconnect_give_up_secs", lambda: float("inf"))

    assert next(handler.consume())["data"] == 42
    assert waits == [1.0, 2.0]
    assert client.call_history["subscribe_calls"] == [("topic", 0)]


def test_mqtt_handler_initial_connection_retries_error_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(monkeypatch, [("connect", False), ("message", make_dummy_message(b"42"))])
    connect = client.connect
    results = iter([mqtt_handler.mqtt.MQTT_ERR_NO_CONN])

    def flaky_connect(*args: Any, **kwargs: Any) -> int:
        try:
            return next(results)
        except StopIteration:
            return connect(*args, **kwargs)

    client.connect = flaky_connect  # type: ignore[method-assign]
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
    waits = record_retry_waits(handler)

    assert next(handler.consume())["data"] == 42
    assert waits == [1.0]
    assert client.call_history["subscribe_calls"] == [("topic", 0)]


def test_mqtt_handler_retries_rejected_handshake_and_failed_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect_error", 5),
            ("connect", False),
            ("connect", False),
            ("message", make_dummy_message(b"42")),
        ],
    )
    subscribe = client.subscribe

    def flaky_subscribe(*args: Any, **kwargs: Any) -> tuple[int, int]:
        if not client.call_history["subscribe_calls"]:
            client.call_history["subscribe_calls"].append((args[0], kwargs["qos"]))
            return mqtt_handler.mqtt.MQTT_ERR_NO_CONN, 0
        return subscribe(*args, **kwargs)

    client.subscribe = flaky_subscribe  # type: ignore[method-assign]
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
    waits = record_retry_waits(handler)

    assert next(handler.consume())["data"] == 42
    assert waits == [1.0, 2.0]
    assert len(client.call_history["connect_calls"]) == 3
    assert len(client.call_history["subscribe_calls"]) == 2


def test_mqtt_handler_close_stops_initial_connection_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(monkeypatch, [])
    client.results["connect"] = mqtt_handler.mqtt.MQTT_ERR_NO_CONN
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
    state = get_handler_attr(handler, "_MqttHandler__consumer_state")

    def close_during_wait(_delay: float) -> bool:
        handler.close_consumer()
        return cast(bool, state.closed.is_set())

    state.closed.wait = close_during_wait

    with pytest.raises(RuntimeError, match="consumer closed"):
        next(handler.consume())
    assert client.call_history["connect_calls"] == [("host", 1883, 25)]
    assert client.call_history["disconnect_calls"] == 1


def test_mqtt_handler_close_during_initial_handshake_stops_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [])
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")

    def close_during_loop(timeout: float = 1.0) -> int:
        _ = timeout
        handler.close_consumer()
        return mqtt_handler.mqtt.MQTT_ERR_SUCCESS

    client.loop = close_during_loop  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="consumer closed"):
        next(handler.consume())
    assert client.call_history["connect_calls"] == [("host", 1883, 25)]
    assert client.call_history["disconnect_calls"] == 1


def test_mqtt_handler_close_during_failed_connect_stops_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [])
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")

    def fail_after_close(*_args: Any, **_kwargs: Any) -> int:
        handler.close_consumer()
        raise OSError("socket closed")

    client.connect = fail_after_close  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="consumer closed"):
        next(handler.consume())
    assert client.call_history["disconnect_calls"] == 1


def test_mqtt_handler_closed_state_skips_connect_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [])
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
    state = get_handler_attr(handler, "_MqttHandler__consumer_state")
    state.closed.set()

    with pytest.raises(RuntimeError, match="consumer closed"):
        next(handler.consume())
    assert not client.call_history["connect_calls"]


def test_mqtt_handler_initial_loop_interrupt_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(monkeypatch, [])
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")

    with pytest.raises(KeyboardInterrupt):
        next(handler.consume())
    assert client.call_history["connect_calls"] == [("host", 1883, 25)]


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


def test_mqtt_handler_reconnect_requires_initialized_consumer() -> None:
    with pytest.raises(RuntimeError, match="not initialized"):
        invoke_handler_method(mqtt_handler.MqttHandler(), "_reconnect_with_backoff", cast(Any, DummyClient()))


def test_mqtt_handler_get_next_message_requires_state_after_client_init(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = mqtt_handler.MqttHandler()

    def _dummy_client() -> DummyClient:
        return DummyClient()

    monkeypatch.setattr(handler, "_ensure_consumer_client", _dummy_client)

    with pytest.raises(RuntimeError, match="not initialized"):
        invoke_handler_method(handler, "_get_next_message")


def test_mqtt_handler_get_next_message_requires_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_mqtt_handler_get_next_message_recovers_from_stored_connect_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
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

    client = DummyClient()
    client.events = [("connect", True), ("message", make_dummy_message(b"42"))]
    client.on_connect = getattr(handler, "_on_connect")
    client.on_message = getattr(handler, "_on_message")

    def _dummy_client() -> DummyClient:
        return client

    monkeypatch.setattr(handler, "_ensure_consumer_client", _dummy_client)

    assert invoke_handler_method(handler, "_get_next_message").payload == b"42"

    assert get_handler_attr(handler, "_MqttHandler__consumer_state").connect_error is None
    assert client.call_history["reconnect_calls"] == 1


def test_mqtt_handler_get_next_message_raises_on_reconnect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MQTT_GIVE_UP_SECS", "0")
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
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
    set_handler_attr(
        handler,
        "_MqttHandler__consumer_state",
        build_consumer_state(client_id="client-id"),
    )

    handler.close_consumer()
    assert get_handler_attr(handler, "_MqttHandler__consumer_state") is None


def test_mqtt_handler_consume_recovers_after_loop_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_mqtt_handler_consume_recovers_after_loop_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("loop_exception", "socket closed"),
            ("connect", True),
            ("message", make_dummy_message(b"42")),
        ],
    )
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")

    assert next(handler.consume())["data"] == 42
    assert client.call_history["reconnect_calls"] == 1


def test_mqtt_handler_close_during_message_loop_stops_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    client = install_dummy_client(monkeypatch, [("connect", False), ("message", make_dummy_message(b"42"))])
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("topic", "host", "user", "pass")
    generator = handler.consume()
    assert next(generator)["data"] == 42

    def close_during_loop(timeout: float = 1.0) -> int:
        _ = timeout
        handler.close_consumer()
        return mqtt_handler.mqtt.MQTT_ERR_SUCCESS

    client.loop = close_during_loop  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="consumer closed"):
        next(generator)
    assert client.call_history["disconnect_calls"] == 1


def test_mqtt_handler_reconnect_retries_after_oserror_and_resubscribes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry an OSError with backoff, then resubscribe if the session was lost."""
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'{"k": 1}', topic="sensor/a")),
            ("disconnect", 1),
            ("connect", False),  # broker lost the session -> resubscribe
            ("message", make_dummy_message(b'{"k": 2}', topic="sensor/a")),
        ],
    )
    failures = iter([OSError("dns failure"), OSError("dns failure")])

    original_reconnect = client.reconnect

    def flaky_reconnect() -> int:
        failure = next(failures, None)
        if failure is not None:
            raise failure
        return original_reconnect()

    client.reconnect = flaky_reconnect  # type: ignore[method-assign]
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("sensor/a", "host", "user", "pass")
    sleeps = record_retry_waits(handler)
    generator = handler.consume()
    first = next(generator)
    second = next(generator)

    assert first["data"] == {"k": 1}
    assert second["data"] == {"k": 2}
    # Two failed attempts slept with exponential backoff before success.
    assert sleeps == [1.0, 2.0]
    # Initial subscribe + post-reconnect resubscribe (session_present=False).
    assert [topic for topic, _qos in client.call_history["subscribe_calls"]] == [
        "sensor/a",
        "sensor/a",
    ]


def test_mqtt_handler_reconnect_gives_up_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prolonged disconnection raises so the worker can restart."""
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'{"k": 1}', topic="sensor/a")),
            ("disconnect", 1),
        ],
    )

    def always_down() -> int:
        raise OSError("broker unreachable")

    client.reconnect = always_down  # type: ignore[method-assign]

    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("sensor/a", "host", "user", "pass")
    generator = handler.consume()
    next(generator)
    monkeypatch.setenv("MQTT_RECONNECT_GIVE_UP_SECS", "0")

    with pytest.raises(RuntimeError, match="could not reconnect within"):
        next(generator)


def test_mqtt_handler_reconnect_retries_on_error_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-success rc from reconnect() is retried, not raised immediately."""
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'{"k": 1}', topic="sensor/a")),
            ("disconnect", 1),
            ("connect", True),  # broker kept the session -> no resubscribe
            ("message", make_dummy_message(b'{"k": 2}', topic="sensor/a")),
        ],
    )
    rcs = iter([mqtt_handler.mqtt.MQTT_ERR_NO_CONN])
    original_reconnect = client.reconnect

    def flaky_reconnect() -> int:
        try:
            return next(rcs)
        except StopIteration:
            return original_reconnect()

    client.reconnect = flaky_reconnect  # type: ignore[method-assign]
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("sensor/a", "host", "user", "pass")
    sleeps = record_retry_waits(handler)
    generator = handler.consume()
    next(generator)
    second = next(generator)

    assert second["data"] == {"k": 2}
    assert sleeps == [1.0]
    assert len(client.call_history["subscribe_calls"]) == 1


def test_mqtt_handler_reconnect_backoff_caps_at_max(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = install_dummy_client(
        monkeypatch,
        [
            ("connect", False),
            ("message", make_dummy_message(b'{"k": 1}', topic="sensor/a")),
            ("disconnect", 1),
            ("connect", True),
            ("message", make_dummy_message(b'{"k": 2}', topic="sensor/a")),
        ],
    )
    failures = iter([OSError("down")] * 7)
    original_reconnect = client.reconnect

    def flaky_reconnect() -> int:
        failure = next(failures, None)
        if failure is not None:
            raise failure
        return original_reconnect()

    client.reconnect = flaky_reconnect  # type: ignore[method-assign]
    handler = mqtt_handler.MqttHandler()
    handler.init_consumer("sensor/a", "host", "user", "pass")
    sleeps = record_retry_waits(handler)
    generator = handler.consume()
    next(generator)
    next(generator)

    assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


def test_mqtt_handler_reconnect_give_up_env_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MQTT_RECONNECT_GIVE_UP_SECS", "not-a-number")
    handler = mqtt_handler.MqttHandler()
    assert invoke_handler_method(handler, "_reconnect_give_up_secs") == mqtt_handler.DEFAULT_RECONNECT_GIVE_UP_SECS
