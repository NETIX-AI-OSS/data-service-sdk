from __future__ import annotations

# pylint: disable=too-many-arguments,too-many-positional-arguments

from typing import Any, Dict, List

import pytest
from redis import ResponseError

from framework.utils import timeseries


class DummyRedisTS:
    def __init__(self) -> None:
        self.created: List[Dict[str, Any]] = []
        self.added: List[Dict[str, Any]] = []
        self.get_value: Any = (1, 2)
        self.revrange_values: List[Any] = [(1, 10), (2, 20)]
        self.raise_on_create = False
        self.raise_on_add = False

    def create(self, key: str, retention_msecs: int, labels: dict[str, str], duplicate_policy: str) -> None:
        if self.raise_on_create:
            raise ResponseError("exists")
        self.created.append({"key": key, "retention": retention_msecs, "labels": labels, "policy": duplicate_policy})

    def add(self, key: str, timestamp: int, value: Any) -> None:
        if self.raise_on_add:
            raise ResponseError("bad")
        self.added.append({"key": key, "timestamp": timestamp, "value": value})

    def get(self, key: str) -> Any:
        return (key, self.get_value)

    def revrange(
        self,
        key: str,
        from_time: str,
        to_time: str,
        count: int | None = None,
        aggregation_type: str | None = None,
        bucket_size_msec: int | None = None,
    ) -> Any:  # pylint: disable=too-many-arguments,too-many-positional-arguments
        _ = (from_time, to_time, count, aggregation_type, bucket_size_msec)
        return [(key, v) for v in self.revrange_values]


class DummyRedisWrapper:
    def __init__(self, ts: DummyRedisTS) -> None:
        self._ts = ts

    def ts(self) -> DummyRedisTS:
        return self._ts


def test_timeseries_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    ts_client = DummyRedisTS()
    wrapper = DummyRedisWrapper(ts_client)

    monkeypatch.setattr(timeseries.RedisTimeseries, "get_redis_instance", lambda: wrapper)
    monkeypatch.setattr(timeseries.Timeseries, "redis_timeseries_producer", wrapper)

    series = timeseries.Timeseries("id-1", {"meta": True}, retention_msecs=100)

    series.add(42, timestamp=123)
    series.add(43)
    series.publish((124, 43))
    value = series.get()
    last = series.get_last_n(2)
    before = series.get_n_before_timestamp(1, "-")
    agg = series.aggregate_last_n(timeseries.AggregationMethod.SUM, bucket_size_secs=2)

    assert ts_client.created
    assert ts_client.added
    assert str(timeseries.AggregationMethod.SUM) == "sum"
    assert value[0] == "id-1"
    assert last
    assert before
    assert agg


def test_timeseries_handles_response_error(monkeypatch: pytest.MonkeyPatch) -> None:
    ts_client = DummyRedisTS()
    ts_client.raise_on_create = True
    ts_client.raise_on_add = True
    wrapper = DummyRedisWrapper(ts_client)

    monkeypatch.setattr(timeseries.RedisTimeseries, "get_redis_instance", lambda: wrapper)
    monkeypatch.setattr(timeseries.Timeseries, "redis_timeseries_producer", wrapper)

    series = timeseries.Timeseries("id-2")
    series.add(99, timestamp=456)


def test_timeseries_query(monkeypatch: pytest.MonkeyPatch) -> None:
    ts_client = DummyRedisTS()
    wrapper = DummyRedisWrapper(ts_client)

    monkeypatch.setattr(timeseries.RedisTimeseries, "get_redis_instance", lambda: wrapper)
    monkeypatch.setattr(timeseries.TimeseriesQuery, "redis_timeseries", wrapper)

    query = timeseries.TimeseriesQuery()
    last = query.query_last(1, count=1)
    current = query.query(1)

    assert last
    assert current
