from __future__ import annotations

from typing import Any, cast

import pytest

from framework.utils import redis_timeseries


def test_redis_timeseries_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}

    class DummyRedisCluster:
        @classmethod
        def from_url(cls, url: str) -> "DummyRedisCluster":
            calls["url"] = url
            return cls()

    monkeypatch.setenv("REDIS_CLUSTER", "true")
    monkeypatch.setenv("REDIS_PASSWORD", "pwd")
    monkeypatch.setenv("REDIS_HOST", "host")
    monkeypatch.setenv("REDIS_PORT", "6379")

    monkeypatch.setattr(redis_timeseries, "RedisCluster", DummyRedisCluster)

    redis_timeseries.RedisTimeseries.cls_instance = None
    instance = redis_timeseries.RedisTimeseries.get_redis_instance()

    assert isinstance(cast(Any, instance), DummyRedisCluster)
    assert calls["url"].startswith("redis://:pwd@host:6379")


def test_redis_timeseries_non_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}

    class DummyRedis:
        @classmethod
        def from_url(cls, url: str) -> "DummyRedis":
            calls["url"] = url
            return cls()

    monkeypatch.setenv("REDIS_CLUSTER", "false")
    monkeypatch.setenv("REDIS_PASSWORD", "pwd")
    monkeypatch.setenv("REDIS_HOST", "host")
    monkeypatch.setenv("REDIS_PORT", "6379")

    monkeypatch.setattr(redis_timeseries, "Redis", DummyRedis)

    redis_timeseries.RedisTimeseries.cls_instance = None
    instance = redis_timeseries.RedisTimeseries.get_redis_instance()

    assert isinstance(cast(Any, instance), DummyRedis)
    assert calls["url"].startswith("redis://:pwd@host:6379")


def test_redis_timeseries_raises_when_instance_missing() -> None:
    class DummyInstance:
        redis_instance = None

    redis_timeseries.RedisTimeseries.cls_instance = DummyInstance()  # type: ignore[assignment]

    with pytest.raises(RuntimeError):
        redis_timeseries.RedisTimeseries.get_redis_instance()
