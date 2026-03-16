from __future__ import annotations

import logging
import time
from typing import Any

from framework.utils.timeseries_manager import TimeseriesManger
from framework.utils.timeseries import Timeseries

logger = logging.getLogger(__name__)


class StateHandler:
    timeseries_manager = TimeseriesManger()

    def get_worker_state_ts_id(self, worker_id: str, state: str) -> str:
        worker_state_ts_id = f"{worker_id}/{state}"
        return worker_state_ts_id

    def get_worker_state_ts(self, worker_id: str, state: str) -> Timeseries:
        worker_state_ts_id = self.get_worker_state_ts_id(worker_id, state)
        timeseries = self.timeseries_manager.get_timeseries_create_if_not_present(worker_state_ts_id)

        return timeseries

    def get_state(self, worker_id: str, state: str, default: Any) -> Any:
        timeseries = self.get_worker_state_ts(worker_id, state)
        value = timeseries.get_last_n(1)
        # try:
        if len(value) == 0:
            return default
        return value[0][1]

    def set_state(self, worker_id: str, state: str, value: Any) -> None:
        timeseries = self.get_worker_state_ts(worker_id, state)
        timeseries.add(value)


class WorkerOffsetHandler:
    state_handler = StateHandler()

    def get_offset(self, worker_id: str, retention_ts_sec: int) -> int:
        now_timestamp = int(time.time() * 1000)
        logger.debug(worker_id)
        logger.debug(retention_ts_sec)
        # logger.debug("now_timestamp")
        # logger.debug(now_timestamp)
        # worker_offset_timestamp = int(
        #     self.state_handler.get_state(
        #         worker_id,
        #         "worker_offset_timestamp",
        #         default=now_timestamp)
        # )

        # # retention sec of timeseries
        # if worker_offset_timestamp < now_timestamp - (retention_ts_sec * 1000):
        #     now_timestamp = now_timestamp - (retention_ts_sec * 1000)

        # logger.debug("now_timestamp")
        # logger.debug(now_timestamp)
        return now_timestamp

    def set_offset(self, worker_id: str, worker_offset_timestamp_new: int) -> None:
        # worker_offset_timestamp_new = worker_offset_timestamp + \
        #     (self.batch_size * self.data_resolution * 1000)
        # logger.debug(worker_offset_timestamp_new)
        logger.debug("worker_offset_timestamp_new")
        logger.debug(worker_offset_timestamp_new)
        self.state_handler.set_state(worker_id, "worker_offset_timestamp", value=worker_offset_timestamp_new)
