# Timeseries Utilities

## Purpose
Provide Redis Timeseries-backed primitives for writing, querying, and aggregating time-series values.

## Key Classes and Functions
- `data_service_sdk.utils.redis_timeseries.RedisTimeseries.get_redis_instance()`
- `data_service_sdk.utils.timeseries.Timeseries`
- `data_service_sdk.utils.timeseries.TimeseriesQuery`
- `data_service_sdk.utils.timeseries.AggregationMethod`
- `data_service_sdk.utils.timeseries_manager.TimeseriesManger`

## Minimal Configuration Example
```python
from data_service_sdk.utils.timeseries import Timeseries, TimeseriesQuery, AggregationMethod
from data_service_sdk.utils.timeseries_manager import TimeseriesManger

series = Timeseries("sensor-1", retention_msecs=3600000)
series.add(21.4)
series.add(22.0)

latest = series.get()
recent = series.get_last_n(10)
window = series.aggregate_last_n(AggregationMethod.AVERAGE, bucket_size_secs=60)

manager = TimeseriesManger()
managed = manager.get_timeseries_create_if_not_present("sensor-1")

query = TimeseriesQuery()
last = query.query_last(ts_id=1, count=1)
```

## Known Constraints and Edge Cases
- `RedisTimeseries` is a singleton wrapper resolved from environment (`REDIS_CLUSTER`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`).
- `Timeseries` creation ignores duplicate-create errors via logged `ResponseError`.
- Retention defaults to `TS_AGGREGATION_INTERVAL_SECS * 1000` when not provided.
- `TimeseriesManger` class name is intentionally misspelled in source and should be referenced exactly.

## Related Links
- [Expression Utility](expression.md)
- [Worker State and Offset](../worker/state-and-offset.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
