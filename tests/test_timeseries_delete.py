from __future__ import annotations

from typing import Any

import pytest

from framework.utils import timeseries as timeseries_module
from framework.utils.timeseries import Timeseries
from framework.utils.timeseries_manager import TimeseriesManger


class DummyTsClient:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []

    def create(self, key: str, retention_msecs: int, labels: dict[str, str], duplicate_policy: str) -> None:
        self.create_calls.append(
            {
                "key": key,
                "retention_msecs": retention_msecs,
                "labels": labels,
                "duplicate_policy": duplicate_policy,
            }
        )


class DummyRedis:
    def __init__(self) -> None:
        self.ts_client = DummyTsClient()
        self.unlinked: list[str] = []
        self.unlink_result = 1

    def ts(self) -> DummyTsClient:
        return self.ts_client

    def unlink(self, *keys: str) -> int:
        self.unlinked.extend(keys)
        return self.unlink_result


@pytest.fixture(name="dummy_redis")
def dummy_redis_fixture(monkeypatch: pytest.MonkeyPatch) -> DummyRedis:
    dummy = DummyRedis()
    monkeypatch.setattr(timeseries_module.RedisTimeseries, "get_redis_instance", classmethod(lambda cls: dummy))
    monkeypatch.setattr(Timeseries, "redis_timeseries_producer", dummy)
    return dummy


def test_delete_unlinks_key(dummy_redis: DummyRedis) -> None:
    series = Timeseries("42", retention_msecs=1000)
    assert series.delete() is True
    assert dummy_redis.unlinked == ["42"]


def test_delete_returns_false_when_key_missing(dummy_redis: DummyRedis) -> None:
    dummy_redis.unlink_result = 0
    series = Timeseries("42", retention_msecs=1000)
    assert series.delete() is False


def test_delete_by_id_skips_create(dummy_redis: DummyRedis) -> None:
    assert Timeseries.delete_by_id(7) is True
    assert dummy_redis.unlinked == ["7"]
    assert dummy_redis.ts_client.create_calls == []


def test_default_retention_used_when_not_passed(dummy_redis: DummyRedis) -> None:
    Timeseries("42")
    (call,) = dummy_redis.ts_client.create_calls
    assert call["retention_msecs"] == timeseries_module.DEFAULT_RETENTION_MSECS
    assert call["duplicate_policy"] == "BLOCK"
    assert call["labels"] == {"id": "42"}


def test_manager_defers_to_default_retention(dummy_redis: DummyRedis) -> None:
    manager = TimeseriesManger()
    manager.get_timeseries_create_if_not_present("42")
    (call,) = dummy_redis.ts_client.create_calls
    assert call["retention_msecs"] == timeseries_module.DEFAULT_RETENTION_MSECS
