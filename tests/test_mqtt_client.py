"""MQTT publisher and client construction without a live broker."""

from __future__ import annotations

import math
import ssl
import threading
import time
from unittest.mock import Mock, call

import paho.mqtt.client as mqtt
import pytest

from data_service_sdk.handlers.utils import mqtt_client  # pylint: disable=import-error
from framework.handlers.utils.mqtt_client import MqttPublishStalledError, MqttPublisher


def test_public_and_legacy_imports_are_the_same_module() -> None:
    assert mqtt_client.MqttPublisher is MqttPublisher


def test_client_factory_configures_v2_auth_tls_and_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = Mock(return_value=Mock())
    monkeypatch.setattr(mqtt_client.mqtt, "Client", factory)
    client = mqtt_client.create_mqtt_client(
        client_id="worker", clean_session=False, username="user", password="secret", tls=True, ca_file="ca.pem"
    )
    factory.assert_called_once_with(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="worker",
        clean_session=False,
        reconnect_on_failure=True,
    )
    client.username_pw_set.assert_called_once_with("user", "secret")
    client.tls_set.assert_called_once_with(
        ca_certs="ca.pem", cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT
    )
    client.reconnect_delay_set.assert_called_once_with(min_delay=1, max_delay=30)
    client.max_queued_messages_set.assert_called_once_with(mqtt_client.MAX_PENDING_PUBLISHES)


def test_give_up_setting_preserves_both_legacy_names(caplog: pytest.LogCaptureFixture) -> None:
    assert math.isinf(mqtt_client.mqtt_give_up_secs({}))
    assert mqtt_client.mqtt_give_up_secs({"MQTT_PUBLISH_GIVE_UP_SECS": "12"}) == 12
    assert mqtt_client.mqtt_give_up_secs({"MQTT_RECONNECT_GIVE_UP_SECS": "13"}) == 13
    assert mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "14", "MQTT_PUBLISH_GIVE_UP_SECS": "12"}) == 14
    assert math.isinf(mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "nan"}))
    assert math.isinf(mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "-1"}))
    assert math.isinf(mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "inf"}))
    assert mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "0"}) == 0
    assert "deprecated" in caplog.text


def test_publisher_resolves_legacy_deadline_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("MQTT_PUBLISH_GIVE_UP_SECS", "12")
    monkeypatch.delenv("MQTT_GIVE_UP_SECS", raising=False)
    client = Mock()
    client.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
    client.publish.return_value.is_published.return_value = True

    publisher = MqttPublisher(client)
    assert publisher.give_up_secs == 12
    publisher.publish("telemetry", "1")
    publisher.publish("telemetry", "2")
    publisher.check_health()
    publisher.check_health()
    assert caplog.text.count("MQTT_PUBLISH_GIVE_UP_SECS is deprecated") == 1

    caplog.clear()
    explicit = MqttPublisher(client, give_up_secs=5)
    explicit.publish("telemetry", "3")
    explicit.check_health()
    assert explicit.give_up_secs == 5
    assert "deprecated" not in caplog.text


def test_client_factory_without_auth_and_with_insecure_tls(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = Mock(return_value=Mock())
    monkeypatch.setattr(mqtt_client.mqtt, "Client", factory)
    client = mqtt_client.create_mqtt_client(tls=True, tls_insecure=True)
    client.username_pw_set.assert_not_called()
    client.tls_set.assert_called_once_with(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set.assert_called_once_with(True)


def test_publisher_keeps_failure_streak_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = iter([10.0, 10.0, 11.0, 12.0])
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: next(clock))
    client = Mock()
    client.publish.return_value.rc = mqtt.MQTT_ERR_NO_CONN
    publisher = MqttPublisher(client, name="gateway", give_up_secs=5)

    assert publisher.publish("cmd", "{}", qos=0) is None
    assert publisher.first_failure_time == 10.0
    assert publisher.publish("cmd", "{}", qos=0) is None
    assert publisher.first_failure_time == 10.0

    client.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
    assert publisher.publish("cmd", "{}", qos=0) is client.publish.return_value
    assert publisher.first_failure_time is None


def test_publisher_raises_after_deadline_with_original_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: 400.0)
    client = Mock()
    client.publish.side_effect = OSError("socket gone")
    publisher = MqttPublisher(client, name="EQ01", give_up_secs=300)
    publisher.first_failure_time = 99.0
    with pytest.raises(MqttPublishStalledError, match="EQ01") as caught:
        publisher.publish("telemetry", "{}")
    assert isinstance(caught.value.__cause__, OSError)


def test_queued_success_without_ack_eventually_stalls(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    info = Mock(rc=mqtt.MQTT_ERR_SUCCESS)
    info.is_published.return_value = False
    client = Mock()
    client.publish.return_value = info
    publisher = MqttPublisher(client, name="gateway", give_up_secs=300)

    assert publisher.publish("cmd", "{}") is info
    now[0] = 301.0
    with pytest.raises(MqttPublishStalledError, match="unacknowledged message"):
        publisher.publish("cmd", "{}")
    assert client.publish.call_count == 1


def test_idle_health_check_catches_unacknowledged_message(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    info = Mock(rc=mqtt.MQTT_ERR_SUCCESS)
    info.is_published.return_value = False
    client = Mock()
    client.publish.return_value = info
    publisher = MqttPublisher(client, give_up_secs=30)
    publisher.publish("cmd", "{}")
    now[0] = 31.0
    with pytest.raises(MqttPublishStalledError, match="unacknowledged message"):
        publisher.check_health()


def test_idle_health_check_catches_failed_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    client = Mock()
    client.publish.return_value.rc = mqtt.MQTT_ERR_NO_CONN
    publisher = MqttPublisher(client, give_up_secs=30)
    publisher.publish("cmd", "{}", qos=0)
    publisher.check_health()  # A recent failure stays within the recovery window.
    now[0] = 31.0
    with pytest.raises(MqttPublishStalledError, match="failed for 31s"):
        publisher.check_health()


def test_acknowledged_message_is_removed_from_pending_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: 10.0)
    info = Mock(rc=mqtt.MQTT_ERR_SUCCESS)
    info.is_published.side_effect = [False, False, True, True]
    client = Mock()
    client.publish.return_value = info
    publisher = MqttPublisher(client, give_up_secs=300)
    publisher.publish("telemetry", "1")
    assert len(getattr(publisher, "_pending")) == 1
    publisher.publish("telemetry", "2")
    assert len(getattr(publisher, "_pending")) == 0


def test_pending_queue_has_a_hard_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_client, "MAX_PENDING_PUBLISHES", 1)
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: 10.0)
    info = Mock(rc=mqtt.MQTT_ERR_SUCCESS)
    info.is_published.return_value = False
    client = Mock()
    client.publish.return_value = info
    publisher = MqttPublisher(client, give_up_secs=300)
    with pytest.raises(MqttPublishStalledError, match="1 pending"):
        publisher.publish("telemetry", "1")


def test_concurrent_health_checks_do_not_pop_the_same_message_twice() -> None:
    info = Mock(rc=mqtt.MQTT_ERR_SUCCESS)
    info.is_published.return_value = False
    client = Mock()
    client.publish.return_value = info
    publisher = MqttPublisher(client, give_up_secs=300)
    publisher.publish("cmd", "{}")

    entered = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    calls = []
    errors = []

    def published() -> bool:
        calls.append(1)
        entered.set()
        assert release.wait(timeout=2)
        return True

    info.is_published.side_effect = published

    def check(started: threading.Event | None = None) -> None:
        if started is not None:
            started.set()
        try:
            publisher.check_health()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            errors.append(exc)

    first = threading.Thread(target=check)
    second = threading.Thread(target=check, args=(second_started,))
    first.start()
    assert entered.wait(timeout=2)
    second.start()
    assert second_started.wait(timeout=2)
    time.sleep(0.05)
    assert len(calls) == 1
    release.set()
    first.join(timeout=2)
    second.join(timeout=2)
    assert not first.is_alive() and not second.is_alive()
    assert not errors


def test_publisher_starts_and_stops_network_loop() -> None:
    client = Mock()
    publisher = MqttPublisher(client)
    publisher.check_health()
    publisher.connect("broker", 1883, keepalive=25)
    client.connect_async.assert_called_once_with("broker", 1883, keepalive=25)
    client.loop_start.assert_called_once_with()
    publisher.stop()
    client.loop_stop.assert_called_once_with()
    client.disconnect.assert_called_once_with()
    assert client.mock_calls.index(call.disconnect()) < client.mock_calls.index(call.loop_stop())


def test_publisher_can_connect_asynchronously() -> None:
    client = Mock()
    publisher = MqttPublisher(client)
    publisher.connect("broker", 8883, asynchronous=True)
    client.connect_async.assert_called_once_with("broker", 8883, keepalive=60)


def test_finite_deadline_keeps_synchronous_initial_connect() -> None:
    client = Mock()
    publisher = MqttPublisher(client, give_up_secs=20)
    publisher.connect("broker")
    client.connect.assert_called_once_with("broker", 1883, keepalive=60)
    client.connect_async.assert_not_called()


def test_explicit_synchronous_connect_remains_available_with_infinite_deadline() -> None:
    client = Mock()
    publisher = MqttPublisher(client)
    publisher.connect("broker", asynchronous=False)
    client.connect.assert_called_once_with("broker", 1883, keepalive=60)
    client.connect_async.assert_not_called()


def test_real_paho_offline_qos_one_is_accepted_and_later_drained() -> None:
    client = mqtt_client.create_mqtt_client()
    publisher = MqttPublisher(client)
    info = publisher.publish("telemetry", "value", qos=1)
    assert info is not None
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
    assert len(getattr(publisher, "_pending")) == 1
    # Paho retains this packet while offline. Its public is_published() still
    # raises after an acknowledgement because the initial rc remains NO_CONN.
    info._set_as_published()  # pylint: disable=protected-access
    publisher.check_health()
    assert len(getattr(publisher, "_pending")) == 0


def test_paho_queue_full_returns_backpressure_when_another_user_filled_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_client, "MAX_PENDING_PUBLISHES", 1)
    client = mqtt_client.create_mqtt_client()
    assert client.publish("other", "first", qos=1).rc == mqtt.MQTT_ERR_NO_CONN
    publisher = MqttPublisher(client)
    assert publisher.publish("telemetry", "second", qos=1) is None
    assert not getattr(publisher, "_pending")


def test_infinite_outage_applies_backpressure_and_recovers_without_clearing_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(mqtt_client, "MAX_PENDING_PUBLISHES", 1)
    info = Mock(rc=mqtt.MQTT_ERR_SUCCESS)
    info.is_published.return_value = False
    client = Mock()
    client.is_connected.return_value = False
    client.publish.return_value = info
    publisher = MqttPublisher(client)
    publisher.connect("broker")
    assert publisher.publish("telemetry", "first") is info
    assert publisher.publish("telemetry", "second") is None
    assert client.publish.call_count == 1
    now[0] = 301.0
    publisher.check_health()
    assert len(getattr(publisher, "_pending")) == 1
    client.disconnect.assert_called_once_with()
    client.loop_stop.assert_called_once_with()
    assert client.connect_async.call_count == 2
    assert client.loop_start.call_count == 2
    publisher.check_health()  # Recovery is throttled during a sustained outage.
    assert client.loop_stop.call_count == 1
    info.is_published.return_value = True
    assert publisher.publish("telemetry", "second") is info
    assert client.publish.call_count == 2


def test_infinite_failed_publish_recovers_and_explicit_stop_cancels_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    client = Mock()
    client.is_connected.return_value = False
    client.publish.return_value.rc = mqtt.MQTT_ERR_NO_CONN
    publisher = MqttPublisher(client)
    publisher.connect("broker")
    assert publisher.publish("telemetry", "value", qos=0) is None
    now[0] = 301.0
    publisher.check_health()
    assert client.loop_stop.call_count == 1
    publisher.stop()
    now[0] = 1000.0
    publisher.check_health()
    assert client.loop_stop.call_count == 2  # Recovery and explicit stop only.
    with pytest.raises(RuntimeError, match="stopped"):
        publisher.publish("telemetry", "after stop")


def test_disconnection_starts_a_new_recovery_window(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    client = Mock()
    client.is_connected.return_value = True
    publisher = MqttPublisher(client)
    publisher.connect("broker")
    now[0] = 500.0
    publisher.check_health()  # A healthy connection clears startup outage time.
    client.is_connected.return_value = False
    publisher.check_health()
    now[0] = 799.0
    publisher.check_health()
    client.loop_stop.assert_not_called()
    now[0] = 801.0
    publisher.check_health()
    client.loop_stop.assert_called_once_with()


def test_recovery_failure_retries_on_later_health_check(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    client = Mock()
    client.is_connected.return_value = False
    client.disconnect.side_effect = [OSError("socket failed"), None]
    publisher = MqttPublisher(client)
    publisher.connect("broker")
    now[0] = 301.0
    publisher.check_health()  # A failed restart is logged, not fatal.
    assert client.loop_stop.call_count == 0
    now[0] = 602.0
    publisher.check_health()
    assert client.loop_stop.call_count == 1


def test_stop_during_recovery_wait_does_not_restart_client(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    client = Mock()
    client.is_connected.return_value = False
    publisher = MqttPublisher(client)
    publisher.connect("broker")
    now[0] = 301.0
    lock = getattr(publisher, "_lifecycle_lock")
    finished = threading.Event()

    def check() -> None:
        publisher.check_health()
        finished.set()

    thread = threading.Thread(target=check)
    with lock:
        thread.start()
        # The health check has selected a recovery and is waiting for the
        # lifecycle lock. Cancellation wins before it can restart the loop.
        for _ in range(1000):
            if getattr(publisher, "_last_recovery_time") is not None:
                break
            time.sleep(0.001)
        assert getattr(publisher, "_last_recovery_time") == 301.0
        with getattr(publisher, "_state_lock"):
            setattr(publisher, "_stopped", True)
            setattr(publisher, "_connection", None)
    thread.join(timeout=2)
    assert finished.is_set()
    client.loop_stop.assert_not_called()


def test_callback_stop_during_recovery_join_does_not_deadlock_or_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    monkeypatch.setattr(mqtt_client.time, "monotonic", lambda: now[0])
    client = Mock()
    client.is_connected.return_value = False
    publisher = MqttPublisher(client)
    publisher.connect("broker")
    now[0] = 301.0
    joining = threading.Event()
    callback_done = threading.Event()

    def loop_stop() -> None:
        if threading.current_thread() is not getattr(client, "_thread"):
            joining.set()
            assert callback_done.wait(timeout=2)

    client.loop_stop.side_effect = loop_stop

    def stop_from_callback() -> None:
        assert joining.wait(timeout=2)
        publisher.stop()
        callback_done.set()

    callback_thread = threading.Thread(target=stop_from_callback, daemon=True)
    setattr(client, "_thread", callback_thread)
    callback_thread.start()
    recovery_thread = threading.Thread(target=publisher.check_health, daemon=True)
    recovery_thread.start()
    recovery_thread.join(timeout=2)
    callback_thread.join(timeout=2)
    assert not recovery_thread.is_alive()
    assert not callback_thread.is_alive()
    assert callback_done.is_set()
    client.connect_async.assert_called_once_with("broker", 1883, keepalive=60)
    publisher.check_health()  # Cancellation also suppresses later watchdog work.
