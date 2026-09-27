"""Exercise offline delivery through real Paho and a loopback MQTT peer."""

from __future__ import annotations

import socket
import threading
import time
from typing import BinaryIO

import paho.mqtt.client as mqtt

from framework.handlers.utils.mqtt_client import MqttPublisher, create_mqtt_client


def _read_packet(stream: BinaryIO) -> tuple[int, bytes]:
    """Read the short MQTT packets used by this test's broker conversation."""
    header = stream.read(2)
    assert len(header) == 2, "connection closed before an MQTT header arrived"
    assert header[1] < 128, "this fixture expects a one-byte remaining length"
    body = stream.read(header[1])
    assert len(body) == header[1]
    return header[0], body


def test_offline_packet_reaches_broker_after_initial_connection_failure() -> None:
    failed = threading.Event()
    acknowledged = threading.Event()
    errors: list[Exception] = []
    client = create_mqtt_client(client_id="offline-recovery-test")
    client.on_connect_fail = lambda _client, _userdata: failed.set()
    client.on_publish = lambda *_args: acknowledged.set()
    publisher = MqttPublisher(client)

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.settimeout(10)
        port = int(listener.getsockname()[1])

        def broker() -> None:
            try:
                connection, _address = listener.accept()
                with connection:
                    connection.settimeout(10)
                    with connection.makefile("rb") as stream:
                        packet_type, _connect = _read_packet(stream)
                        assert packet_type == 0x10
                        connection.sendall(b"\x20\x02\x00\x00")  # Successful CONNACK.
                        packet_type, packet = _read_packet(stream)
                        assert packet_type == 0x32  # QoS 1, first delivery, not retained.
                        topic_length = int.from_bytes(packet[:2], "big")
                        assert packet[2 : 2 + topic_length] == b"telemetry"
                        message_id = packet[2 + topic_length : 4 + topic_length]
                        assert packet[4 + topic_length :] == b"queued-during-outage"
                        connection.sendall(b"\x40\x02" + message_id)
                        # Stop must disconnect, rather than republish an already
                        # accepted packet after receiving its acknowledgement.
                        assert _read_packet(stream) == (0xE0, b"")
            except Exception as error:  # pylint: disable=broad-exception-caught
                errors.append(error)

        worker = threading.Thread(target=broker, daemon=True)
        try:
            info = publisher.publish("telemetry", "queued-during-outage", qos=1)
            assert info is not None and info.rc == mqtt.MQTT_ERR_NO_CONN
            publisher.connect("127.0.0.1", port)
            assert failed.wait(10), "initial connection must fail while the port is not listening"
            listener.listen()
            worker.start()
            assert acknowledged.wait(10), "Paho must retry and deliver the queued packet"
            # Paho calls on_publish just before marking MQTTMessageInfo done.
            deadline = time.monotonic() + 5
            while getattr(publisher, "_pending") and time.monotonic() < deadline:
                publisher.check_health()
                time.sleep(0.01)
            assert not getattr(publisher, "_pending")
        finally:
            publisher.stop()
            if worker.ident is not None:
                worker.join(timeout=11)
        assert not worker.is_alive()
        assert not errors
