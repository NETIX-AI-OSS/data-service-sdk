from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, Protocol, TypedDict


class ConditionConfig(TypedDict):
    equation: list[str]


class TransitionConfig(TypedDict):
    trigger: str
    source: str
    dest: str
    conditions_config: list[ConditionConfig]


class StateMachineConfig(TypedDict):
    states: list[str]
    transitions: dict[str, TransitionConfig]


class ExpressionConfig(TypedDict):
    equation: list[str]


class TsdbTableColumn(TypedDict, total=False):
    col_name: str
    type: str
    pkey: bool


class TsdbOutputConfig(TypedDict):
    user: str
    password: str
    host: str
    port: str
    db: str
    table_name: str
    table_schema: list[TsdbTableColumn]


class KafkaOutputConfig(TypedDict):
    bootstrap_servers: str
    topic: str


class ApiOutputConfig(TypedDict):
    url: str
    url_batch: str
    timeout: float


class MqttConfig(TypedDict):
    topic: str
    host: str
    user: str
    password: str


MqttPayloadFormat = Literal["json", "raw_bytes"]


class MqttConsumeError(TypedDict):
    type: str
    message: str


class MqttPacketMetadata(TypedDict):
    topic: str
    subscription_topic: str
    host: str
    qos: int | None
    retain: bool
    mid: int | None
    dup: bool
    payload_size_bytes: int
    received_at_ms: int
    received_at_iso: str
    payload_format: MqttPayloadFormat


class MqttConsumedMessage(TypedDict):
    data: Any | None
    metadata: MqttPacketMetadata
    error: MqttConsumeError | None


KafkaPayloadFormat = Literal["json", "raw_bytes"]


class KafkaConsumeError(TypedDict):
    type: str
    message: str


class KafkaHeaderMetadata(TypedDict):
    key: str
    value_text: str | None
    value_bytes_b64: str | None


class KafkaPacketMetadata(TypedDict):
    topic: str
    subscription_topic: str
    partition: int | None
    offset: int | None
    timestamp: int | None
    timestamp_type: int | None
    key_text: str | None
    key_bytes_b64: str | None
    headers: list[KafkaHeaderMetadata]
    payload_size_bytes: int
    received_at_ms: int
    received_at_iso: str
    payload_format: KafkaPayloadFormat


class KafkaConsumedMessage(TypedDict):
    data: Any | bytes | None
    metadata: KafkaPacketMetadata
    error: KafkaConsumeError | None


class OutputProducer(Protocol):
    def produce(self, msg: Any) -> Any:
        """Deliver one message to the configured output."""


class OutputProducerFactory(Protocol):
    def __call__(self, config: Mapping[str, Any]) -> OutputProducer:
        """Build an output producer from its configuration."""


class RowMapping(Protocol):
    def __getitem__(self, key: str) -> Any:
        """Read one field from a row by name."""


RowMappings = Sequence[Mapping[str, Any]]
