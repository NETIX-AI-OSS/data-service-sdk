from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional, cast

from paho.mqtt import subscribe, publish
from framework.types import MqttConsumeError, MqttConsumedMessage, MqttPacketMetadata, MqttPayloadFormat

logger = logging.getLogger(__name__)


class MqttHandler:
    __topic_name: Optional[str] = None
    __host: Optional[str] = None
    __auth: Optional[Dict[str, str]] = None

    def init_consumer(self, topic: str, host: str, user: str, password: str) -> None:
        self.__auth = {"username": user, "password": password}
        self.__topic_name = topic
        self.__host = host

    @staticmethod
    def _extract_message_metadata(
        msg_obj: Any, subscription_topic: str, host: str, payload: bytes
    ) -> MqttPacketMetadata:
        current_time = datetime.now(timezone.utc)
        return {
            "topic": str(getattr(msg_obj, "topic", "") or ""),
            "subscription_topic": subscription_topic,
            "host": host,
            "qos": cast(Optional[int], getattr(msg_obj, "qos", None)),
            "retain": bool(getattr(msg_obj, "retain", False)),
            "mid": cast(Optional[int], getattr(msg_obj, "mid", None)),
            "dup": bool(getattr(msg_obj, "dup", False)),
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

    def consume(self) -> Generator[MqttConsumedMessage, None, None]:
        logger.debug("Mqtt consume created")
        if self.__topic_name is None or self.__host is None or self.__auth is None:
            raise RuntimeError("MQTT consumer not initialized")
        topic = self.__topic_name
        host = self.__host
        auth = self.__auth
        while True:
            msg = None
            try:
                msg = subscribe.simple(
                    topic,
                    hostname=host,
                    auth=auth,
                    msg_count=1,
                    retained=True,
                    keepalive=25,
                    client_id=os.uname().nodename,
                    clean_session=False,
                )
                if isinstance(msg, list):
                    if not msg:
                        continue
                    msg_obj = msg[0]
                else:
                    msg_obj = msg
                payload = self._normalize_payload(getattr(msg_obj, "payload", b""))
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
            except TypeError as error:
                logger.warning("Error : %s, payload : %s", str(error), msg)

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
