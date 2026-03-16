from __future__ import annotations

# pylint: disable=no-member,too-many-arguments,too-many-positional-arguments

from typing import Any, Dict, cast

import pytest

from framework.handlers.subscription import factory as subscription_factory
from framework.handlers.subscription.manager import SubscriptionManager
from framework.handlers.subscription.type import SUBSCRIPTION_TYPE


class DummyKafkaHandler:
    def __init__(self) -> None:
        self.init_args: tuple[str, str, str, str, bool, float] | None = None

    def init_consumer(
        self,
        topic: str,
        kafka_bootstrap_servers: str,
        kafka_ts_group_id: str,
        kafka_ts_offset_reset: str,
        kafka_ts_auto_commit: bool,
        polling_interval_secs: float,
    ) -> None:  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self.init_args = (
            topic,
            kafka_bootstrap_servers,
            kafka_ts_group_id,
            kafka_ts_offset_reset,
            kafka_ts_auto_commit,
            polling_interval_secs,
        )


class DummyMqttHandler:
    def __init__(self) -> None:
        self.init_args: tuple[str, str, str, str] | None = None

    def init_consumer(self, topic: str, host: str, user: str, password: str) -> None:
        self.init_args = (topic, host, user, password)


def test_subscription_factory_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subscription_factory, "kafka_handler", type("K", (), {"KafkaHandler": DummyKafkaHandler}))

    config: Dict[str, Any] = {
        "kafka_topic_subscribe": "t",
        "kafka_bootstrap_servers": "b1,b2",
        "kafka_ts_group_id": "g",
        "kafka_ts_offset_reset": "earliest",
        "kafka_ts_auto_commit": True,
        "polling_interval_secs": 5.0,
    }
    controller = cast(
        DummyKafkaHandler, subscription_factory.Factory.get_subscription_controller(SUBSCRIPTION_TYPE["kafka"], config)
    )

    assert controller.init_args is not None
    assert controller.init_args[0] == "t"


def test_subscription_factory_mqtt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subscription_factory, "mqtt_handler", type("M", (), {"MqttHandler": DummyMqttHandler}))

    config: Dict[str, Any] = {"topic": "t", "host": "h", "user": "u", "password": "p"}
    controller = cast(
        DummyMqttHandler, subscription_factory.Factory.get_subscription_controller(SUBSCRIPTION_TYPE["mqtt"], config)
    )

    assert controller.init_args == ("t", "h", "u", "p")


def test_subscription_manager_sets_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy = object()
    monkeypatch.setattr(subscription_factory.Factory, "get_subscription_controller", lambda *_args, **_kwargs: dummy)

    manager = SubscriptionManager()
    controller = manager.get_controller({"type": "1"})

    assert controller is dummy
    assert manager.subscription_controller is dummy


def test_subscription_factory_unknown() -> None:
    controller = subscription_factory.Factory.get_subscription_controller("unknown", {})
    assert controller is None
