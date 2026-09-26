"""MQTT client construction and a watchdog for long-running publishers."""

from __future__ import annotations

import logging
import math
import os
import ssl
import threading
import time
from collections import deque
from collections.abc import Mapping
from typing import Any

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

DEFAULT_GIVE_UP_SECS = 300.0
MAX_PENDING_PUBLISHES = 4096
GIVE_UP_ENV = "MQTT_GIVE_UP_SECS"
LEGACY_GIVE_UP_ENVS = ("MQTT_PUBLISH_GIVE_UP_SECS", "MQTT_RECONNECT_GIVE_UP_SECS")


def mqtt_give_up_secs(environ: Mapping[str, str] | None = None) -> float:
    """Read the common deadline, honoring both older deployment settings.

    The canonical setting wins when more than one name is present. An invalid
    value falls back to the default rather than disabling the watchdog.
    """
    values = os.environ if environ is None else environ
    for name in (GIVE_UP_ENV, *LEGACY_GIVE_UP_ENVS):
        if name in values:
            try:
                seconds = float(values[name])
                if seconds < 0:
                    raise ValueError("negative deadline")
                if not math.isfinite(seconds):
                    raise ValueError("non-finite deadline")
                if name != GIVE_UP_ENV:
                    logger.warning("%s is deprecated; use %s", name, GIVE_UP_ENV)
                return seconds
            except ValueError:
                logger.warning("Invalid %s=%r; using %ss", name, values[name], DEFAULT_GIVE_UP_SECS)
                return DEFAULT_GIVE_UP_SECS
    return DEFAULT_GIVE_UP_SECS


def create_mqtt_client(  # pylint: disable=too-many-arguments
    *,
    client_id: str = "",
    clean_session: bool = True,
    username: str | None = None,
    password: str | None = None,
    tls: bool = False,
    ca_file: str | None = None,
    tls_insecure: bool = False,
) -> mqtt.Client:
    """Build a paho v2 client with consistent auth, TLS and reconnect bounds."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        clean_session=clean_session,
        reconnect_on_failure=True,
    )
    if username:
        client.username_pw_set(username, password)
    if tls:
        if tls_insecure:
            client.tls_set(cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)
        else:
            client.tls_set(
                ca_certs=ca_file or None,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )
            client.tls_insecure_set(False)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


class MqttPublishStalledError(RuntimeError):
    """A publisher has failed on every attempt for the configured deadline."""


class MqttPublisher:
    """Keep a paho client alive and fail visibly after a sustained publish outage.

    A successful paho return only queues the packet. This class checks paho's
    delivery state on subsequent publishes or a periodic `check_health()` call,
    and fails if the oldest packet is
    still unsent or unacknowledged after the deadline. Callers requiring a
    synchronous broker acknowledgement can wait on the returned info.
    """

    def __init__(self, client: mqtt.Client, *, name: str = "MQTT", give_up_secs: float | None = None) -> None:
        self.client = client
        self.name = name
        self.give_up_secs = give_up_secs
        self.first_failure_time: float | None = None
        self.last_error: Exception | None = None
        self._pending: deque[tuple[mqtt.MQTTMessageInfo, float]] = deque()
        self._state_lock = threading.RLock()

    def check_health(self) -> None:
        """Check delivery progress from an idle worker's regular poll loop."""
        with self._state_lock:
            give_up_secs = self.give_up_secs if self.give_up_secs is not None else mqtt_give_up_secs()
            self._check_pending(give_up_secs)
            if self.first_failure_time is not None:
                stalled_for = time.monotonic() - self.first_failure_time
                if stalled_for >= give_up_secs:
                    raise MqttPublishStalledError(
                        f"Publishing for {self.name} has failed for {stalled_for:.0f}s; last error: {self.last_error}"
                    ) from self.last_error

    def _check_pending(self, give_up_secs: float) -> None:
        while self._pending and self._pending[0][0].is_published():
            self._pending.popleft()
        if not self._pending:
            return
        stalled_for = time.monotonic() - self._pending[0][1]
        if stalled_for >= give_up_secs or len(self._pending) >= MAX_PENDING_PUBLISHES:
            raise MqttPublishStalledError(
                f"Publishing for {self.name} has an unacknowledged message for {stalled_for:.0f}s "
                f"({len(self._pending)} pending, give-up at {give_up_secs:.0f}s)"
            )

    def connect(self, host: str, port: int = 1883, *, keepalive: int = 60, asynchronous: bool = False) -> None:
        if asynchronous:
            self.client.connect_async(host, port, keepalive=keepalive)
        else:
            self.client.connect(host, port, keepalive=keepalive)
        self.client.loop_start()

    def publish(self, topic: str, payload: Any, *, qos: int = 1, retain: bool = False) -> mqtt.MQTTMessageInfo | None:
        with self._state_lock:
            return self._publish_locked(topic, payload, qos=qos, retain=retain)

    def _publish_locked(self, topic: str, payload: Any, *, qos: int, retain: bool) -> mqtt.MQTTMessageInfo | None:
        give_up_secs = self.give_up_secs if self.give_up_secs is not None else mqtt_give_up_secs()
        self._check_pending(give_up_secs)
        error: Exception | None = None
        try:
            info = self.client.publish(topic, payload, qos=qos, retain=retain)
            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                self.first_failure_time = None
                self.last_error = None
                if not info.is_published():
                    self._pending.append((info, time.monotonic()))
                    self._check_pending(give_up_secs)
                return info
            error = RuntimeError(f"publish returned {mqtt.error_string(info.rc)}")
        except OSError as exc:
            error = exc
        self.last_error = error

        if self.first_failure_time is None:
            self.first_failure_time = time.monotonic()
        stalled_for = time.monotonic() - self.first_failure_time
        logger.warning(
            "MQTT publish to %s failed (%s); stalled for %.0fs (give-up at %.0fs)",
            topic,
            error,
            stalled_for,
            give_up_secs,
        )
        if stalled_for >= give_up_secs:
            raise MqttPublishStalledError(
                f"Publishing for {self.name} has failed for {stalled_for:.0f}s; last error: {error}"
            ) from error
        return None

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()
        with self._state_lock:
            self._pending.clear()
