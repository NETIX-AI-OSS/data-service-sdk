from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Protocol, cast

from redis import ResponseError

from framework.utils.redis_timeseries import RedisTimeseries

logger = logging.getLogger(__name__)

TS_AGGREGATION_INTERVAL_SECS = int(os.environ.get("TS_AGGREGATION_INTERVAL_SECS", "60"))
# Retention applied to newly created series when the caller does not pass one.
# Consumers surface this via their deployment config (e.g. data-service's
# k8s configmap) so every write path shares a single retention source of truth.
DEFAULT_RETENTION_MSECS = int(os.environ.get("REDIS_TS_RETENTION_MS", str(3 * 60 * 60 * 1000)))


class AggregationMethod(str, Enum):
    AVERAGE = "avg"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    RANGE = "range"
    TIME_WEIGHTED_AVERAGE = "twa"

    def __str__(self) -> str:
        return str(self.value)

    #  Further supported methods: `std.p`, `std.s`, `var.p`, `var.s`, `twa`


class RedisTimeseriesClient(Protocol):
    def create(self, key: str, retention_msecs: int, labels: dict[str, str], duplicate_policy: str) -> Any:
        pass

    def add(self, key: str, timestamp: int, value: Any, retention_msecs: Optional[int] = None) -> Any:
        pass

    def get(self, key: str | int) -> Any:
        pass

    def revrange(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        key: str | int,
        from_time: str,
        to_time: str | int,
        count: Optional[int] = None,
        aggregation_type: Optional[str] = None,
        bucket_size_msec: Optional[int] = None,
    ) -> Any:
        pass


class RedisTimeseriesWrapper(Protocol):
    def ts(self) -> RedisTimeseriesClient:
        pass

    def unlink(self, *keys: str) -> Any:
        pass


# redis based timeseries class
@dataclass
class Timeseries:
    # info: TimeseriesInfo
    redis_timeseries_producer: RedisTimeseriesWrapper = cast(
        RedisTimeseriesWrapper, RedisTimeseries.get_redis_instance()
    )

    def __init__(
        self, ts_id: str, meta: Optional[dict[str, Any]] = None, retention_msecs: Optional[int] = None
    ) -> None:
        if retention_msecs is None:
            retention_msecs = DEFAULT_RETENTION_MSECS
        _ = meta
        self.retention_msecs = retention_msecs
        self.id = str(ts_id)  # pylint: disable=invalid-name
        logger.debug("ts init : %s", self.id)
        labels = {"id": self.id}
        redis_timeseries_producer = RedisTimeseries.get_redis_instance()
        try:
            redis_timeseries_producer.ts().create(
                self.id, retention_msecs=retention_msecs, labels=labels, duplicate_policy="BLOCK"
            )
        except ResponseError as exc:
            logger.warning(str(exc))

    def add(self, raw_value: Any, timestamp: Optional[int] = None) -> None:
        if timestamp is None:
            timestamp = int(datetime.now().timestamp() * 1000)
        try:
            self.publish((timestamp, raw_value))
        except ResponseError as exc:
            logger.warning(str(exc))

    def publish(self, data_tuple: tuple[int, Any]) -> None:
        # retention_msecs rides along so a key auto-created by TS.ADD (e.g.
        # re-created by a live writer right after GC deleted it) still gets
        # retention instead of the module default of 0 (= keep forever).
        self.redis_timeseries_producer.ts().add(
            self.id, data_tuple[0], data_tuple[1], retention_msecs=self.retention_msecs
        )

    def get(self) -> Any:
        return self.redis_timeseries_producer.ts().get(self.id)

    def get_last_n(self, count: int) -> List[Any]:
        return_list = cast(
            List[Any], self.redis_timeseries_producer.ts().revrange(self.id, from_time="-", to_time="+", count=count)
        )
        return return_list

    def get_n_before_timestamp(self, count: int, to_timestamp: str | int) -> List[Any]:
        return_list = cast(
            List[Any],
            self.redis_timeseries_producer.ts().revrange(self.id, from_time="-", to_time=to_timestamp, count=count),
        )
        return return_list

    def delete(self) -> bool:
        """Remove this series key entirely (samples, labels and metadata).

        Uses ``UNLINK`` so reclamation happens off the main Redis thread.
        Returns ``True`` when the key existed. Note that constructing a
        ``Timeseries`` creates the key, so prefer :meth:`delete_by_id` when
        you only hold an id and don't want a create-then-delete round trip.
        """
        return bool(self.redis_timeseries_producer.unlink(self.id))

    @staticmethod
    def delete_by_id(ts_id: str | int) -> bool:
        """Remove the series key for *ts_id* without creating it first."""
        return bool(RedisTimeseries.get_redis_instance().unlink(str(ts_id)))

    def aggregate_last_n(
        self, aggregation_method: AggregationMethod, bucket_size_secs: int = TS_AGGREGATION_INTERVAL_SECS
    ) -> List[Any]:
        return_list = cast(
            List[Any],
            self.redis_timeseries_producer.ts().revrange(
                self.id,
                from_time="-",
                to_time="+",
                aggregation_type=aggregation_method,
                bucket_size_msec=bucket_size_secs * 1000,
            ),
        )
        return return_list


class TimeseriesQuery:
    redis_timeseries: RedisTimeseriesWrapper = cast(RedisTimeseriesWrapper, RedisTimeseries.get_redis_instance())

    def query_last(self, ts_id: int, count: int = 1) -> List[Any]:
        return cast(List[Any], self.redis_timeseries.ts().revrange(ts_id, from_time="-", to_time="+", count=count))

    def query(self, ts_id: int) -> Any:
        return self.redis_timeseries.ts().get(ts_id)
