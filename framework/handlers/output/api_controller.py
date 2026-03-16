from __future__ import annotations

import logging
from typing import Any, Sequence
import requests

from framework.types import ApiOutputConfig

logger = logging.getLogger(__name__)


class ApiOutputController:
    def __init__(self, config: ApiOutputConfig) -> None:
        logger.debug("TsdbOutput Controller COnfig")
        logger.debug(config)
        self.url = config["url"]
        self.url_batch = config["url_batch"]
        self.timeout = config["timeout"]

    def produce(self, msg: Any) -> None:
        try:
            response = requests.post(self.url, json=msg, timeout=self.timeout)
            logger.debug(response)
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Error : %s", error)

    def batch_produce(self, msgs: Sequence[Any]) -> None:
        try:
            response = requests.post(self.url_batch, json=msgs, timeout=self.timeout)
            logger.debug(response)
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Error : %s", error)
