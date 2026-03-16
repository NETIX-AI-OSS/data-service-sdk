from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional

from .timeseries import Timeseries

logger = logging.getLogger(__name__)


@dataclass
class TimeseriesManger:
    timeseries_map: Dict[str, Timeseries] = field(default_factory=dict)

    def __init__(self) -> None:
        self.timeseries_map = {}

    def get_timeseries_create_if_not_present(
        self, ts_id: str, ts_meta: Optional[dict] = None, retention_msecs: int = 3600000
    ) -> Timeseries:
        time_series = self.timeseries_map.get(ts_id)
        logger.info(f"created or got redis timeseries: {ts_id}")  # pylint: disable=logging-fstring-interpolation
        if time_series is None:
            time_series = Timeseries(ts_id, ts_meta, retention_msecs)
            self.timeseries_map[ts_id] = time_series
        return time_series
