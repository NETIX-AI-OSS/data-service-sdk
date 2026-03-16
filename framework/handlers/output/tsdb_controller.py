from __future__ import annotations

import logging
from typing import Any, Mapping, Sequence

from framework.handlers.utils.db_handler import TableHandler, DataHandler
from framework.types import TsdbOutputConfig

logger = logging.getLogger(__name__)


class TsdbOutputController:
    def __init__(self, config: TsdbOutputConfig) -> None:
        logger.debug("TsdbOutput Controller COnfig")
        logger.debug(config)
        self.config = config

        table_handler = TableHandler(config["user"], config["password"], config["host"], config["port"], config["db"])

        table_handler.make_table_if_not_exists(config["table_name"], config["table_schema"])
        # self.data_handler = DataHandler(
        #     config['user'],
        #     config['password'],
        #     config['host'],
        #     config['port'],
        #     config['db'])  # db data handler
        self.set_table(self.config)

    def set_table(self, config: TsdbOutputConfig) -> None:
        self.data_handler = DataHandler(
            config["user"], config["password"], config["host"], config["port"], config["db"]
        )  # db data handler
        self.data_handler.set_table(config["table_name"])

    def produce(self, msg: Mapping[str, Any]) -> None:
        try:
            self.data_handler.insert_tsdb([msg])
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Error : %s", error)
            # retry
            self.set_table(self.config)

    def batch_produce(self, msgs: Sequence[Mapping[str, Any]]) -> None:
        try:
            self.data_handler.insert_tsdb(msgs)
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Error : %s", error)
