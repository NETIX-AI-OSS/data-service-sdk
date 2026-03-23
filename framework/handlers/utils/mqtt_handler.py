from __future__ import annotations

import json
import logging
import os
import queue
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional, cast

import paho.mqtt.client as mqtt
from paho.mqtt import publish
from framework.types import MqttConsumeError, MqttConsumedMessage, MqttPacketMetadata, MqttPayloadFormat

logger = logging.getLogger(__name__)


@dataclass
class _MqttConsumerState:
    client_id: str
    client: mqtt.Client | None = None
    messages: queue.Queue[Any] = field(default_factory=queue.Queue)
    connected: bool = False
    connect_error: RuntimeError | None = None


class MqttHandler:
    __topic_name: Optional[str] = None
    __host: Optional[str] = None
    __auth: Optional[Dict[str, str]] = None
    __consumer_state: Optional[_MqttConsumerState] = None

    def init_consumer(self, topic: str, host: str, user: str, password: str) -> None:
        self.close_consumer()
        self.__auth = {"username": user, "password": password}
        self.__topic_name = topic
        self.__host = host
        self.__consumer_state = _MqttConsumerState(client_id=self._build_consumer_client_id(topic, host))

    def _build_consumer_client_id(self, topic: str, host: str) -> str:
        seed = f"{os.uname().nodename}:{os.getpid()}:{id(self)}:{topic}:{host}".encode("utf-8", errors="replace")
        digest = hashlib.sha1(seed).hexdigest()[:16]
        return f"dsdk-{digest}"

    @staticmethod
    def _extract_message_metadata(
        msg_obj: Any, subscription_topic: str, host: str, payload: bytes
    ) -> MqttPacketMetadata:
        current_time = datetime.now(timezone.utc)
        return {
            "topic": str(msg_obj.topic or ""),
            "subscription_topic": subscription_topic,
            "host": host,
            "qos": cast(Optional[int], msg_obj.qos),
            "retain": bool(msg_obj.retain),
            "mid": cast(Optional[int], msg_obj.mid),
            "dup": bool(msg_obj.dup),
            "payload_size_bytes": len(payload),
            "received_at_ms": int(current_time.timestamp() * 1000),
            "received_at_iso": current_time.isoformat(),
            "payload_format": "json",
        }

    @staticmethod
    def _parse_payload(payload: bytes) -> tuple[Any | None, MqttConsumeError | None, MqttPayloadFormat]:
        try:
            return json.loads(payload), None, "json"
        except UnicodeDecodeError as error:
            return (
                payload,
                {"type": "unicode_decode_error", "message": str(error)},
                "raw_bytes",
            )
        except json.JSONDecodeError as error:
            return (
                payload,
                {"type": "json_decode_error", "message": str(error)},
                "raw_bytes",
            )

    @staticmethod
    def _normalize_payload(payload: Any) -> bytes:
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, bytearray):
            return bytes(payload)
        if payload is None:
            return b""
        return str(payload).encode("utf-8", errors="replace")

    def _on_connect(
        self, client: mqtt.Client, _userdata: Any, flags: mqtt.ConnectFlags, reason_code: Any, _properties: Any
    ) -> None:
        state = self.__consumer_state
        if state is None:
            return
        if reason_code != 0:
            state.connect_error = RuntimeError(f"MQTT consumer connection failed: {reason_code}")
            return

        state.connected = True
        session_present = bool(flags.session_present)
        if session_present:
            logger.debug("MQTT consumer resumed existing session topic=%s", self.__topic_name)
            return

        if self.__topic_name is None:
            state.connect_error = RuntimeError("MQTT consumer topic not configured")
            return

        result, _mid = client.subscribe(self.__topic_name, qos=0)
        if result != mqtt.MQTT_ERR_SUCCESS:
            state.connect_error = RuntimeError(f"MQTT consumer subscribe failed: {mqtt.error_string(result)}")
            return

        logger.debug("MQTT consumer subscribed topic=%s", self.__topic_name)

    def _on_disconnect(
        self, _client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any
    ) -> None:
        if self.__consumer_state is not None:
            self.__consumer_state.connected = False
        if reason_code not in (0, None):
            logger.warning("MQTT consumer disconnected: %s", reason_code)

    def _on_message(self, _client: mqtt.Client, _userdata: Any, message: Any) -> None:
        if self.__consumer_state is not None:
            self.__consumer_state.messages.put(message)

    def _ensure_consumer_client(self) -> mqtt.Client:
        state = self.__consumer_state
        if self.__topic_name is None or self.__host is None or self.__auth is None or state is None:
            raise RuntimeError("MQTT consumer not initialized")

        if state.client is not None:
            return state.client

        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=state.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=False,
            reconnect_on_failure=False,
        )
        client.enable_logger(logger)
        client.username_pw_set(self.__auth["username"], self.__auth["password"])
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=30)

        state.messages = queue.Queue()
        state.connected = False
        state.connect_error = None
        state.client = client

        result = client.connect(self.__host, keepalive=25)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT consumer connect failed: {mqtt.error_string(result)}")

        self._wait_for_connection(client)
        return client

    def _wait_for_connection(self, client: mqtt.Client) -> None:
        state = self.__consumer_state
        if state is None:
            raise RuntimeError("MQTT consumer not initialized")

        while not state.connected:
            if state.connect_error is not None:
                error = state.connect_error
                state.connect_error = None
                raise error

            result = client.loop(timeout=1.0)
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT consumer loop failed during connect: {mqtt.error_string(result)}")

    def _get_next_message(self) -> Any:
        client = self._ensure_consumer_client()
        state = self.__consumer_state
        if state is None:
            raise RuntimeError("MQTT consumer not initialized")
        if state.messages is None:
            raise RuntimeError("MQTT consumer message queue not initialized")

        while True:
            if not state.messages.empty():
                return state.messages.get()

            if state.connect_error is not None:
                error = state.connect_error
                state.connect_error = None
                raise error

            if not state.connected:
                result = client.reconnect()
                if result != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError(f"MQTT consumer reconnect failed: {mqtt.error_string(result)}")
                self._wait_for_connection(client)
                continue

            result = client.loop(timeout=1.0)
            if result != mqtt.MQTT_ERR_SUCCESS:
                logger.warning("MQTT consumer loop returned %s, reconnecting", mqtt.error_string(result))
                state.connected = False

    def close_consumer(self) -> None:
        if self.__consumer_state is None:
            return
        if self.__consumer_state.client is not None:
            self.__consumer_state.client.disconnect()
        self.__consumer_state = None

    def consume(self) -> Generator[MqttConsumedMessage, None, None]:
        logger.debug("Mqtt consume created")
        if self.__topic_name is None or self.__host is None or self.__auth is None:
            raise RuntimeError("MQTT consumer not initialized")

        topic = self.__topic_name
        host = self.__host
        while True:
            msg_obj = self._get_next_message()
            payload = self._normalize_payload(msg_obj.payload)
            metadata = self._extract_message_metadata(msg_obj, topic, host, payload)
            data, parse_error, payload_format = self._parse_payload(payload)
            metadata["payload_format"] = payload_format

            consumed_message: MqttConsumedMessage = {
                "data": data,
                "metadata": metadata,
                "error": parse_error,
            }

            if parse_error is None:
                logger.debug(
                    "MQTT message consumed topic=%s format=%s payload_size=%s",
                    metadata["topic"],
                    metadata["payload_format"],
                    metadata["payload_size_bytes"],
                )
            else:
                logger.warning(
                    "MQTT payload parse issue topic=%s format=%s error=%s",
                    metadata["topic"],
                    metadata["payload_format"],
                    parse_error["type"],
                )

            yield consumed_message

    def init_producer(self, topic: str, host: str, user: str, password: str) -> None:
        self.__auth = {"username": user, "password": password}
        self.__topic_name = topic
        self.__host = host

    def produce(self, msg: Any) -> None:
        logger.debug("Mqtt publisher created")
        if self.__topic_name is None or self.__host is None or self.__auth is None:
            raise RuntimeError("MQTT producer not initialized")
        topic = self.__topic_name
        host = self.__host
        auth = self.__auth
        try:
            publish.single(
                topic,
                payload=json.dumps(msg),
                qos=1,  # Quality of Service (0, 1, or 2)
                hostname=host,
                auth=cast(Any, auth),  # Use the authentication credentials
                tls=None,  # You can configure TLS/SSL here if needed
                client_id="",  # Client ID (leave empty for automatic generation)
            )
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Error : %s", error)

    def flush(self) -> None:
        return None
