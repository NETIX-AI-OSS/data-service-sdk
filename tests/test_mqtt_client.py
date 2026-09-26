"""MQTT publisher and client construction without a live broker."""

from __future__ import annotations

import ssl
import threading
import time
from unittest.mock import Mock

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


def test_give_up_setting_preserves_both_legacy_names(caplog: pytest.LogCaptureFixture) -> None:
    assert mqtt_client.mqtt_give_up_secs({"MQTT_PUBLISH_GIVE_UP_SECS": "12"}) == 12
    assert mqtt_client.mqtt_give_up_secs({"MQTT_RECONNECT_GIVE_UP_SECS": "13"}) == 13
    assert mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "14", "MQTT_PUBLISH_GIVE_UP_SECS": "12"}) == 14
    assert mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "nan"}) == 300
    assert mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "-1"}) == 300
    assert mqtt_client.mqtt_give_up_secs({"MQTT_GIVE_UP_SECS": "inf"}) == 300
    assert "deprecated" in caplog.text


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

    assert publisher.publish("cmd", "{}") is None
    assert publisher.first_failure_time == 10.0
    assert publisher.publish("cmd", "{}") is None
    assert publisher.first_failure_time == 10.0

    client.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
    assert publisher.publish("cmd", "{}") is client.publish.return_value
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
    publisher.publish("cmd", "{}")
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
    client.connect.assert_called_once_with("broker", 1883, keepalive=25)
    client.loop_start.assert_called_once_with()
    publisher.stop()
    client.loop_stop.assert_called_once_with()
    client.disconnect.assert_called_once_with()


def test_publisher_can_connect_asynchronously() -> None:
    client = Mock()
    publisher = MqttPublisher(client)
    publisher.connect("broker", 8883, asynchronous=True)
    client.connect_async.assert_called_once_with("broker", 8883, keepalive=60)
