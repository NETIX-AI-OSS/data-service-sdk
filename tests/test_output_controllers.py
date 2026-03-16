from __future__ import annotations

# pylint: disable=no-member

from typing import Any, Dict, List, cast

import pytest

from framework.handlers.output import api_controller, kafka_controller, tsdb_controller


class DummyKafkaHandler:
    def __init__(self) -> None:
        self.produced: List[Any] = []
        self.flushed = False
        self.topic_name: str | None = None
        self.bootstrap_servers: str | None = None

    def init_producer(self, topic_name: str, bootstrap_servers: str) -> None:
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers

    def produce(self, key: str, value: Any) -> None:
        self.produced.append((key, value))

    def flush(self) -> None:
        self.flushed = True


def test_kafka_output_controller_produce_and_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kafka_controller, "KafkaHandler", DummyKafkaHandler)
    controller = kafka_controller.KafkaOutputController({"bootstrap_servers": "b", "topic": "t"})

    controller.produce({"id": 1, "v": "x"})
    controller.batch_produce([{"id": 2}, {"id": 3}])

    handler = cast(DummyKafkaHandler, controller.kafka_handler)
    assert handler.produced == [("1", {"id": 1, "v": "x"}), ("2", {"id": 2}), ("3", {"id": 3})]
    assert handler.flushed is True


def test_kafka_output_controller_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingKafkaHandler(DummyKafkaHandler):
        def produce(self, key: str, value: Any) -> None:
            _ = (key, value)
            raise RuntimeError("boom")

    monkeypatch.setattr(kafka_controller, "KafkaHandler", FailingKafkaHandler)
    controller = kafka_controller.KafkaOutputController({"bootstrap_servers": "b", "topic": "t"})

    controller.produce({"id": 1})
    controller.batch_produce([{"id": 2}])


def test_api_output_controller_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[Dict[str, Any]] = []

    def fake_post(url: str, json: Any, timeout: float) -> str:
        calls.append({"url": url, "json": json, "timeout": timeout})
        return "ok"

    monkeypatch.setattr(api_controller.requests, "post", fake_post)

    controller = api_controller.ApiOutputController({"url": "u", "url_batch": "ub", "timeout": 1.5})
    controller.produce({"id": 1})
    controller.batch_produce([{"id": 2}])

    assert calls == [
        {"url": "u", "json": {"id": 1}, "timeout": 1.5},
        {"url": "ub", "json": [{"id": 2}], "timeout": 1.5},
    ]


def test_api_output_controller_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(_url: str, _json: Any, _timeout: float) -> None:
        raise RuntimeError("fail")

    monkeypatch.setattr(api_controller.requests, "post", fake_post)

    controller = api_controller.ApiOutputController({"url": "u", "url_batch": "ub", "timeout": 1.5})
    controller.produce({"id": 1})
    controller.batch_produce([{"id": 2}])


def test_tsdb_output_controller_retries_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyTableHandler:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self.tables: List[Any] = []

        def make_table_if_not_exists(self, table_name: str, _schema: Any) -> None:
            self.tables.append(table_name)

    class DummyDataHandler:
        created = 0

        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            DummyDataHandler.created += 1
            self.inserted: List[Any] = []

        def set_table(self, _table_name: str) -> None:
            return None

        def insert_tsdb(self, _msgs: Any) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(tsdb_controller, "TableHandler", DummyTableHandler)
    monkeypatch.setattr(tsdb_controller, "DataHandler", DummyDataHandler)

    controller = tsdb_controller.TsdbOutputController(
        {
            "user": "u",
            "password": "p",
            "host": "h",
            "port": "5432",
            "db": "d",
            "table_name": "tbl",
            "table_schema": [],
        }
    )

    controller.produce({"id": 1})

    assert DummyDataHandler.created >= 2


def test_tsdb_output_controller_batch_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyTableHandler:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def make_table_if_not_exists(self, _table_name: str, _schema: Any) -> None:
            return None

    class DummyDataHandler:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self.inserted: List[Any] = []

        def set_table(self, _table_name: str) -> None:
            return None

        def insert_tsdb(self, msgs: Any) -> None:
            self.inserted.append(msgs)

    monkeypatch.setattr(tsdb_controller, "TableHandler", DummyTableHandler)
    monkeypatch.setattr(tsdb_controller, "DataHandler", DummyDataHandler)

    controller = tsdb_controller.TsdbOutputController(
        {
            "user": "u",
            "password": "p",
            "host": "h",
            "port": "5432",
            "db": "d",
            "table_name": "tbl",
            "table_schema": [],
        }
    )

    controller.batch_produce([{"id": 1}])

    assert cast(Any, controller.data_handler).inserted == [[{"id": 1}]]


def test_tsdb_output_controller_batch_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyTableHandler:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def make_table_if_not_exists(self, _table_name: str, _schema: Any) -> None:
            return None

    class DummyDataHandler:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def set_table(self, _table_name: str) -> None:
            return None

        def insert_tsdb(self, _msgs: Any) -> None:
            raise RuntimeError("fail")

    monkeypatch.setattr(tsdb_controller, "TableHandler", DummyTableHandler)
    monkeypatch.setattr(tsdb_controller, "DataHandler", DummyDataHandler)

    controller = tsdb_controller.TsdbOutputController(
        {
            "user": "u",
            "password": "p",
            "host": "h",
            "port": "5432",
            "db": "d",
            "table_name": "tbl",
            "table_schema": [],
        }
    )

    controller.batch_produce([{"id": 1}])
