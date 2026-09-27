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

DEFAULT_GIVE_UP_SECS = math.inf
MAX_PENDING_PUBLISHES = 4096
STALE_RECOVERY_SECS = 300.0
GIVE_UP_ENV = "MQTT_GIVE_UP_SECS"
LEGACY_GIVE_UP_ENVS = ("MQTT_PUBLISH_GIVE_UP_SECS", "MQTT_RECONNECT_GIVE_UP_SECS")


def mqtt_give_up_secs(environ: Mapping[str, str] | None = None) -> float:
    """Read the common deadline, honoring both older deployment settings.

    The canonical setting wins when more than one name is present. An invalid
    value falls back to indefinite recovery. Finite values remain opt-in.
    """
    values = os.environ if environ is None else environ
    for name in (GIVE_UP_ENV, *LEGACY_GIVE_UP_ENVS):
        if name in values:
            try:
                seconds = float(values[name])
                if seconds < 0:
                    raise ValueError("negative deadline")
                if math.isnan(seconds):
                    raise ValueError("NaN deadline")
                if name != GIVE_UP_ENV:
                    logger.warning("%s is deprecated; use %s", name, GIVE_UP_ENV)
                return seconds
            except ValueError:
                logger.warning("Invalid %s=%r; using indefinite recovery", name, values[name])
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
    # Paho otherwise retains an unbounded number of QoS 1/2 messages offline.
    client.max_queued_messages_set(MAX_PENDING_PUBLISHES)
    return client


class MqttPublishStalledError(RuntimeError):
    """A publisher has failed on every attempt for the configured deadline."""


# Delivery state and network-loop lifecycle must stay on this one publisher.
class MqttPublisher:  # pylint: disable=too-many-instance-attributes
    """Keep a paho client alive; finite give-up deadlines remain opt-in.

    A successful paho return only queues the packet. This class checks paho's
    delivery state on subsequent publishes or a periodic `check_health()` call.
    A finite deadline raises if delivery stalls; the default restarts the
    network loop and keeps accepted packets queued. Callers must retry a
    publish that returns None. A returned info reports queue acceptance,
    though Paho's wait methods can raise for packets queued while offline.
    """

    def __init__(self, client: mqtt.Client, *, name: str = "MQTT", give_up_secs: float | None = None) -> None:
        self.client = client
        self.name = name
        self.give_up_secs = mqtt_give_up_secs() if give_up_secs is None else give_up_secs
        self.first_failure_time: float | None = None
        self.last_error: Exception | None = None
        self._pending: deque[tuple[mqtt.MQTTMessageInfo, float]] = deque()
        self._state_lock = threading.RLock()
        self._lifecycle_lock = threading.Lock()
        self._connection: tuple[str, int, int] | None = None
        self._disconnected_since: float | None = None
        self._last_recovery_time: float | None = None
        self._stopped = False

    def check_health(self) -> None:
        """Check delivery progress from an idle worker's regular poll loop."""
        with self._state_lock:
            if self._stopped:
                return
            self._check_pending(self.give_up_secs)
            if self.first_failure_time is not None and math.isfinite(self.give_up_secs):
                stalled_for = time.monotonic() - self.first_failure_time
                if stalled_for >= self.give_up_secs:
                    raise MqttPublishStalledError(
                        f"Publishing for {self.name} has failed for {stalled_for:.0f}s; last error: {self.last_error}"
                    ) from self.last_error
        self._recover_if_stalled()

    @staticmethod
    def _is_published(info: mqtt.MQTTMessageInfo) -> bool:
        if info.rc == mqtt.MQTT_ERR_NO_CONN:
            # Paho retains QoS 1/2 packets after NO_CONN and later marks the
            # info as published, but its public is_published() keeps raising
            # because the original rc remains NO_CONN.
            return bool(getattr(info, "_published", False))
        return info.is_published()

    def _check_pending(self, give_up_secs: float) -> None:
        while self._pending and self._is_published(self._pending[0][0]):
            self._pending.popleft()
        if not self._pending:
            return
        stalled_for = time.monotonic() - self._pending[0][1]
        if stalled_for >= give_up_secs or (math.isfinite(give_up_secs) and len(self._pending) >= MAX_PENDING_PUBLISHES):
            raise MqttPublishStalledError(
                f"Publishing for {self.name} has an unacknowledged message for {stalled_for:.0f}s "
                f"({len(self._pending)} pending, give-up at {give_up_secs:.0f}s)"
            )

    def connect(self, host: str, port: int = 1883, *, keepalive: int = 60, asynchronous: bool | None = None) -> None:
        with self._lifecycle_lock:
            self._connection = (host, port, keepalive)
            self._stopped = False
            self._disconnected_since = time.monotonic()
            # Infinite recovery starts Paho's retry-first-connection loop.
            # Explicit finite deadlines retain the old synchronous default.
            if asynchronous is None:
                asynchronous = math.isinf(self.give_up_secs)
            if asynchronous:
                self.client.connect_async(host, port, keepalive=keepalive)
            else:
                self.client.connect(host, port, keepalive=keepalive)
            self.client.loop_start()

    def publish(self, topic: str, payload: Any, *, qos: int = 1, retain: bool = False) -> mqtt.MQTTMessageInfo | None:
        self._recover_if_stalled()
        with self._state_lock:
            return self._publish_locked(topic, payload, qos=qos, retain=retain)

    def _publish_locked(self, topic: str, payload: Any, *, qos: int, retain: bool) -> mqtt.MQTTMessageInfo | None:
        if self._stopped:
            raise RuntimeError(f"MQTT publisher {self.name} has been stopped")
        give_up_secs = self.give_up_secs
        self._check_pending(give_up_secs)
        if len(self._pending) >= MAX_PENDING_PUBLISHES:
            logger.warning("MQTT pending queue for %s is full; caller must retry the unaccepted message", self.name)
            return None
        error: Exception | None = None
        try:
            info = self.client.publish(topic, payload, qos=qos, retain=retain)
            if info.rc == mqtt.MQTT_ERR_SUCCESS or (info.rc == mqtt.MQTT_ERR_NO_CONN and qos > 0):
                self.first_failure_time = None
                self.last_error = None
                if not self._is_published(info):
                    self._pending.append((info, time.monotonic()))
                    self._check_pending(give_up_secs)
                return info
            if info.rc == mqtt.MQTT_ERR_QUEUE_SIZE:
                logger.warning(
                    "MQTT outgoing queue for %s is full; caller must retry the unaccepted message", self.name
                )
                return None
            error = RuntimeError(f"publish returned {mqtt.error_string(info.rc)}")
        except OSError as exc:
            error = exc
        self.last_error = error

        if self.first_failure_time is None:
            self.first_failure_time = time.monotonic()
        stalled_for = time.monotonic() - self.first_failure_time
        give_up_label = f"{give_up_secs:.0f}s" if math.isfinite(give_up_secs) else "never"
        logger.warning(
            "MQTT publish to %s failed (%s); stalled for %.0fs (give-up at %s)",
            topic,
            error,
            stalled_for,
            give_up_label,
        )
        if stalled_for >= give_up_secs:
            raise MqttPublishStalledError(
                f"Publishing for {self.name} has failed for {stalled_for:.0f}s; last error: {error}"
            ) from error
        return None

    def _recover_if_stalled(self) -> None:
        """Restart a stuck network loop without discarding Paho's QoS queue."""
        if math.isfinite(self.give_up_secs):
            return
        with self._state_lock:
            if self._stopped or self._connection is None:
                return
            # An acknowledgement may have arrived since the previous poll.
            while self._pending and self._is_published(self._pending[0][0]):
                self._pending.popleft()
            now = time.monotonic()
            if self.client.is_connected():
                self._disconnected_since = None
            elif self._disconnected_since is None:
                self._disconnected_since = now
            oldest = self._pending[0][1] if self._pending else None
            stalls = [
                value for value in (oldest, self.first_failure_time, self._disconnected_since) if value is not None
            ]
            if not stalls or now - min(stalls) < STALE_RECOVERY_SECS:
                return
            if self._last_recovery_time is not None and now - self._last_recovery_time < STALE_RECOVERY_SECS:
                return
            self._last_recovery_time = now
        # Never hold _state_lock across loop_stop(): Paho may be executing a
        # callback while this call joins its network thread.
        with self._lifecycle_lock:
            connection = self._active_connection()
            if connection is None:
                return
            host, port, keepalive = connection
            logger.warning("MQTT publisher %s stalled; restarting its network loop", self.name)
            try:
                self.client.disconnect()
                self.client.loop_stop()
                # A user callback may have called stop() from the Paho thread
                # while loop_stop() waited for that thread to finish.
                if self._active_connection() is None:
                    return
                self.client.connect_async(host, port, keepalive=keepalive)
                self.client.loop_start()
            except OSError as exc:
                logger.warning("MQTT publisher %s recovery failed; will retry: %s", self.name, exc)

    def _active_connection(self) -> tuple[str, int, int] | None:
        """Recheck cancellation after waiting for the lifecycle lock."""
        return None if self._stopped else self._connection

    def stop(self) -> None:
        if threading.current_thread() is getattr(self.client, "_thread", None):
            # Paho invokes user callbacks while holding its outgoing-message
            # mutex. Do not take SDK locks here: another thread may hold one
            # while joining this network thread or calling client.publish().
            self._stopped = True
            self.client.disconnect()
            self.client.loop_stop()  # Paho does not join its own thread.
            return
        with self._lifecycle_lock:
            self._stopped = True
            self.client.disconnect()
            self.client.loop_stop()
            with self._state_lock:
                self._pending.clear()
                self._connection = None
