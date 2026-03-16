from __future__ import annotations

import base64
import json
import logging
import time
from datetime import datetime, timezone

from typing import Any, Generator, Optional

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from framework.types import (
    KafkaConsumeError,
    KafkaConsumedMessage,
    KafkaHeaderMetadata,
    KafkaPacketMetadata,
    KafkaPayloadFormat,
)

logger = logging.getLogger(__name__)


class KafkaHandler:
    __topic_name: Optional[str] = None
    __producer: Optional[KafkaProducer] = None
    __consumer: Optional[KafkaConsumer] = None
    __polling_interval_secs: Optional[float] = None
    __subscription_topic: Optional[str] = None

    @staticmethod
    def _normalize_payload(payload: Any) -> bytes:
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, bytearray):
            return bytes(payload)
        if payload is None:
            return b""
        if isinstance(payload, str):
            return payload.encode("utf-8", errors="replace")
        return str(payload).encode("utf-8", errors="replace")

    @staticmethod
    def _try_decode_utf8(value: bytes | None) -> str | None:
        if value is None:
            return None
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return None

    @staticmethod
    def _to_b64(value: bytes | None) -> str | None:
        if value is None:
            return None
        return base64.b64encode(value).decode("ascii")

    @staticmethod
    def _extract_key_and_headers(msg_obj: Any) -> tuple[str | None, str | None, list[KafkaHeaderMetadata]]:
        key_value = getattr(msg_obj, "key", None)
        key_bytes = None if key_value is None else KafkaHandler._normalize_payload(key_value)
        key_text = KafkaHandler._try_decode_utf8(key_bytes)
        key_bytes_b64 = KafkaHandler._to_b64(key_bytes)

        headers_value = getattr(msg_obj, "headers", [])
        headers: list[KafkaHeaderMetadata] = []
        if isinstance(headers_value, list):
            for header in headers_value:
                if not isinstance(header, tuple) or len(header) != 2:
                    continue
                header_key, header_value = header
                raw_header_value: bytes | None
                if header_value is None:
                    raw_header_value = None
                else:
                    raw_header_value = KafkaHandler._normalize_payload(header_value)
                headers.append(
                    {
                        "key": str(header_key),
                        "value_text": KafkaHandler._try_decode_utf8(raw_header_value),
                        "value_bytes_b64": KafkaHandler._to_b64(raw_header_value),
                    }
                )

        return key_text, key_bytes_b64, headers

    @staticmethod
    def _parse_payload(payload: bytes) -> tuple[Any | bytes | None, KafkaConsumeError | None, KafkaPayloadFormat]:
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
    def _extract_kafka_metadata(
        msg_obj: Any, payload: bytes, subscription_topic: str, key_text: str | None, key_bytes_b64: str | None
    ) -> KafkaPacketMetadata:
        current_time = datetime.now(timezone.utc)
        _partition = getattr(msg_obj, "partition", None)
        _offset = getattr(msg_obj, "offset", None)
        _timestamp = getattr(msg_obj, "timestamp", None)
        _timestamp_type = getattr(msg_obj, "timestamp_type", None)
        return {
            "topic": str(getattr(msg_obj, "topic", "") or ""),
            "subscription_topic": subscription_topic,
            "partition": _partition if isinstance(_partition, int) else None,
            "offset": _offset if isinstance(_offset, int) else None,
            "timestamp": _timestamp if isinstance(_timestamp, int) else None,
            "timestamp_type": _timestamp_type if isinstance(_timestamp_type, int) else None,
            "key_text": key_text,
            "key_bytes_b64": key_bytes_b64,
            "headers": [],
            "payload_size_bytes": len(payload),
            "received_at_ms": int(current_time.timestamp() * 1000),
            "received_at_iso": current_time.isoformat(),
            "payload_format": "json",
        }

    def _to_consumed_message(self, msg_obj: Any) -> KafkaConsumedMessage:
        payload = self._normalize_payload(getattr(msg_obj, "value", b""))
        data, parse_error, payload_format = self._parse_payload(payload)
        subscription_topic = self.__subscription_topic or str(getattr(msg_obj, "topic", "") or "")
        key_text, key_bytes_b64, headers = self._extract_key_and_headers(msg_obj)
        metadata = self._extract_kafka_metadata(msg_obj, payload, subscription_topic, key_text, key_bytes_b64)
        metadata["headers"] = headers
        metadata["payload_format"] = payload_format
        return {"data": data, "metadata": metadata, "error": parse_error}

    def init_consumer(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        topic: str,
        kafka_bootstrap_servers: str,
        kafka_ts_group_id: str,
        kafka_ts_offset_reset: str,
        kafka_ts_auto_commit: bool,
        polling_interval_secs: float,
    ) -> None:
        self.__subscription_topic = topic
        self.__polling_interval_secs = polling_interval_secs
        self.__consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap_servers.split(","),
            group_id=kafka_ts_group_id,
            auto_offset_reset=kafka_ts_offset_reset,
            enable_auto_commit=kafka_ts_auto_commit,
        )

    def assign_and_seek(
        self, start: int, end: int, topic: str, kafka_bootstrap_servers: str
    ) -> Generator[KafkaConsumedMessage, None, None]:
        self.__subscription_topic = topic
        self.__consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_servers.split(","),
        )
        partition = TopicPartition(topic, 0)
        # start = self.start
        # end = self.end
        self.__consumer.assign([partition])
        self.__consumer.seek(partition, start)
        for msg in self.__consumer:
            if msg.offset > end:
                break
            consumed_message = self._to_consumed_message(msg)
            if consumed_message["error"] is None:
                logger.debug("Kafka message consumed topic=%s", consumed_message["metadata"]["topic"])
            else:
                logger.warning(
                    "Kafka payload parse issue topic=%s error=%s",
                    consumed_message["metadata"]["topic"],
                    consumed_message["error"]["type"] if consumed_message["error"] else "unknown",
                )
            yield consumed_message

    def consume(self) -> Generator[KafkaConsumedMessage, None, None]:
        if self.__consumer is None or self.__polling_interval_secs is None:
            raise RuntimeError("Kafka consumer not initialized")
        while True:
            for msg in self.__consumer:
                consumed_message = self._to_consumed_message(msg)
                if consumed_message["error"] is None:
                    logger.debug("Kafka message consumed topic=%s", consumed_message["metadata"]["topic"])
                else:
                    logger.warning(
                        "Kafka payload parse issue topic=%s error=%s",
                        consumed_message["metadata"]["topic"],
                        consumed_message["error"]["type"] if consumed_message["error"] else "unknown",
                    )
                yield consumed_message
            logger.warning(
                "Kafka Consumer Stopped. Sleeping for interval before retry: %s", str(self.__polling_interval_secs)
            )
            time.sleep(self.__polling_interval_secs)

    def init_producer(self, topic_name: str, bootstrap_servers: str) -> None:
        self.__topic_name = topic_name
        self.__producer = KafkaProducer(
            retries=5,
            acks="all",
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def produce(self, key: str, value: Any) -> Any:
        if self.__producer is None or self.__topic_name is None:
            raise RuntimeError("Kafka producer not initialized")
        return self.__producer.send(self.__topic_name, key=key.encode(encoding="UTF-8"), value=value)

    def flush(self) -> None:
        if self.__producer is None:
            raise RuntimeError("Kafka producer not initialized")
        self.__producer.flush()
