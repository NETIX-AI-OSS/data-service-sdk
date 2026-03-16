from __future__ import annotations

from typing import Any, Optional

import pytest

from framework.utils import timeseries_manager


class DummyTimeseries:
    def __init__(
        self, ts_id: str, meta: Optional[dict[str, Any]] = None, retention_msecs: Optional[int] = None
    ) -> None:
        self.id = ts_id
        self.meta = meta
        self.retention_msecs = retention_msecs


def test_timeseries_manager_reuses_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(timeseries_manager, "Timeseries", DummyTimeseries)
    manager = timeseries_manager.TimeseriesManger()

    first = manager.get_timeseries_create_if_not_present("ts-1", {"k": "v"}, 1234)
    second = manager.get_timeseries_create_if_not_present("ts-1", {"k": "other"}, 9999)

    assert isinstance(first, DummyTimeseries)
    assert first is second
    assert first.retention_msecs == 1234
