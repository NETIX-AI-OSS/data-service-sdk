from __future__ import annotations

import base64
import json
import logging
import os
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

# ``consume()`` drives the consumer through kafka-python's iterator protocol:
# one ``poll()`` fetches up to ``max_poll_records`` records, which are then
# yielded one at a time with no further ``poll()`` in between. The *whole*
# batch therefore has to be processed within ``max_poll_interval_ms`` or the
# heartbeat thread declares the poll timeout expired and leaves the group.
#
# With auto-commit that is silently destructive: offsets are only committed
# during ``poll()``, so a member evicted mid-batch has committed nothing,
# rejoins at the same offset, replays the same records, and stalls again --
# a rebalance loop that makes zero progress while lag grows without bound.
# kafka-python's stock 500 records / 5 minutes leaves only ~600ms per record
# before that trips, which a slow downstream dependency blows straight
# through (prod incident 2026-08-14, consumer group tag-redis-nc3).
#
# Smaller batches plus a longer ceiling give a slow batch far more headroom
# to finish and commit. Both are env-tunable so a wedged consumer can be
# widened without a rebuild.
MAX_POLL_RECORDS = int(os.environ.get("KAFKA_MAX_POLL_RECORDS", "100"))
MAX_POLL_INTERVAL_MS = int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", "600000"))


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
        key_bytes = None if msg_obj.key is None else KafkaHandler._normalize_payload(msg_obj.key)
        key_text = KafkaHandler._try_decode_utf8(key_bytes)
        key_bytes_b64 = KafkaHandler._to_b64(key_bytes)

        headers_value = msg_obj.headers
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
        return {
            "topic": str(msg_obj.topic or ""),
            "subscription_topic": subscription_topic,
            "partition": msg_obj.partition if isinstance(msg_obj.partition, int) else None,
            "offset": msg_obj.offset if isinstance(msg_obj.offset, int) else None,
            "timestamp": msg_obj.timestamp if isinstance(msg_obj.timestamp, int) else None,
            "timestamp_type": msg_obj.timestamp_type if isinstance(msg_obj.timestamp_type, int) else None,
            "key_text": key_text,
            "key_bytes_b64": key_bytes_b64,
            "headers": [],
            "payload_size_bytes": len(payload),
            "received_at_ms": int(current_time.timestamp() * 1000),
            "received_at_iso": current_time.isoformat(),
            "payload_format": "json",
        }

    def _to_consumed_message(self, msg_obj: Any) -> KafkaConsumedMessage:
        payload = self._normalize_payload(msg_obj.value if msg_obj.value is not None else b"")
        data, parse_error, payload_format = self._parse_payload(payload)
        subscription_topic = self.__subscription_topic or str(msg_obj.topic or "")
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
        max_poll_records: int = MAX_POLL_RECORDS,
        max_poll_interval_ms: int = MAX_POLL_INTERVAL_MS,
    ) -> None:
        self.__subscription_topic = topic
        self.__polling_interval_secs = polling_interval_secs
        self.__consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap_servers.split(","),
            group_id=kafka_ts_group_id,
            auto_offset_reset=kafka_ts_offset_reset,
            enable_auto_commit=kafka_ts_auto_commit,
            max_poll_records=max_poll_records,
            max_poll_interval_ms=max_poll_interval_ms,
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
