from __future__ import annotations

# pylint: disable=no-member

from typing import Any, Dict, List, cast

import pytest

from framework.handlers.output import factory as output_factory
from framework.handlers.output.manager import OutputManager
from framework.handlers.output.type import OUTPUT_CONTROLLER_TYPE


class DummyKafkaController:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config


class DummyTsdbController:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config


class DummyApiController:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config


class DummyMqttHandler:
    def __init__(self) -> None:
        self.init_args: tuple[str, str, str, str] | None = None

    def init_producer(self, topic: str, host: str, user: str, password: str) -> None:
        self.init_args = (topic, host, user, password)

    def produce(self, msg: Any) -> None:
        _ = msg


def test_output_factory_creates_kafka_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(output_factory.kafka_controller, "KafkaOutputController", DummyKafkaController)

    config: Dict[str, Any] = {"bootstrap_servers": "b", "topic": "t"}
    controller = output_factory.Factory.get_output_controller(OUTPUT_CONTROLLER_TYPE["kafka"], config)

    assert isinstance(controller, DummyKafkaController)


def test_output_factory_creates_tsdb_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(output_factory.tsdb_controller, "TsdbOutputController", DummyTsdbController)

    config: Dict[str, Any] = {
        "user": "u",
        "password": "p",
        "host": "h",
        "port": "5432",
        "db": "d",
        "table_name": "tbl",
        "table_schema": [],
    }
    controller = output_factory.Factory.get_output_controller(OUTPUT_CONTROLLER_TYPE["tsdb"], config)

    assert isinstance(controller, DummyTsdbController)


def test_output_factory_creates_api_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(output_factory.api_controller, "ApiOutputController", DummyApiController)

    config: Dict[str, Any] = {"url": "u", "url_batch": "ub", "timeout": 1.0}
    controller = output_factory.Factory.get_output_controller(OUTPUT_CONTROLLER_TYPE["api"], config)

    assert isinstance(controller, DummyApiController)


def test_output_factory_creates_mqtt_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(output_factory, "MqttHandler", DummyMqttHandler)

    config: Dict[str, Any] = {"topic": "t", "host": "h", "user": "u", "password": "p"}
    controller = cast(
        DummyMqttHandler, output_factory.Factory.get_output_controller(OUTPUT_CONTROLLER_TYPE["mqtt"], config)
    )

    assert controller.init_args == ("t", "h", "u", "p")


def test_output_manager_produce_and_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    produced: List[Any] = []

    class DummyProducer:
        def produce(self, msg: Any) -> None:
            produced.append(msg)

    monkeypatch.setattr(output_factory.Factory, "get_output_controller", lambda *_args, **_kwargs: DummyProducer())

    manager = OutputManager()
    manager.get_controller({"type": "any"})

    manager.produce({"id": 1})
    manager.batch_produce([{"id": 2}, {"id": 3}])

    assert produced == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_output_manager_rejects_unknown_type(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(output_factory.Factory, "get_output_controller", lambda *_args, **_kwargs: None)

    manager = OutputManager()

    with pytest.raises(ValueError):
        manager.get_controller({"type": "unknown"})


def test_output_manager_init_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(output_factory.Factory, "get_output_controller", lambda *_args, **_kwargs: DummyMqttHandler())

    manager = OutputManager()
    manager.init_manager([{"config": {"type": "mqtt"}}])

    assert len(manager.output_controller_list) == 1


def test_output_factory_unknown_returns_none() -> None:
    controller = output_factory.Factory.get_output_controller("unknown", {})
    assert controller is None


def test_output_factory_mqtt_missing_field_raises() -> None:
    with pytest.raises(ValueError):
        output_factory.Factory.get_output_controller(OUTPUT_CONTROLLER_TYPE["mqtt"], {"host": "h"})
