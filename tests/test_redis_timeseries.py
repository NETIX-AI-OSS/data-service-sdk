from __future__ import annotations

from typing import Any, cast

import pytest

from framework.utils import redis_timeseries


def test_redis_timeseries_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, Any] = {}

    class DummyRedisCluster:
        @classmethod
        def from_url(cls, url: str, **kwargs: Any) -> "DummyRedisCluster":
            calls["url"] = url
            calls["kwargs"] = kwargs
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
    # Regression: dynamic startup nodes must stay off. With redis-py's default
    # of True the client swaps the configured DNS endpoint for the pod IPs it
    # discovers, and can never re-resolve the service once those IPs churn.
    assert calls["kwargs"]["dynamic_startup_nodes"] is False
    assert calls["kwargs"]["socket_connect_timeout"] > 0
    assert calls["kwargs"]["socket_timeout"] > 0


def test_redis_timeseries_non_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, Any] = {}

    class DummyRedis:
        @classmethod
        def from_url(cls, url: str, **kwargs: Any) -> "DummyRedis":
            calls["url"] = url
            calls["kwargs"] = kwargs
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
    # Single-node clients take no startup-node set, but must still bound
    # connect/read so an unreachable host fails fast.
    assert "dynamic_startup_nodes" not in calls["kwargs"]
    assert calls["kwargs"]["socket_connect_timeout"] > 0
    assert calls["kwargs"]["socket_timeout"] > 0


def test_redis_timeseries_raises_when_instance_missing() -> None:
    class DummyInstance:
        redis_instance = None

    redis_timeseries.RedisTimeseries.cls_instance = DummyInstance()  # type: ignore[assignment]

    with pytest.raises(RuntimeError):
        redis_timeseries.RedisTimeseries.get_redis_instance()
