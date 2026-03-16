from __future__ import annotations

import logging
from typing import Any, Mapping

from framework.handlers.utils.kafka_handler import KafkaHandler
from framework.types import KafkaOutputConfig

logger = logging.getLogger(__name__)


class KafkaOutputController:
    def __init__(self, config: KafkaOutputConfig) -> None:
        logger.debug("KafkaOutput Controller Config")
        logger.debug(config)
        bootstrap_servers = config["bootstrap_servers"]
        topic = config["topic"]
        self.kafka_handler = KafkaHandler()
        self.kafka_handler.init_producer(topic_name=topic, bootstrap_servers=bootstrap_servers)
        self.topic = topic

    def produce(self, msg: Mapping[str, Any]) -> None:
        try:
            self.kafka_handler.produce(key=str(msg["id"]), value=msg)
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Error : %s", error)

    def batch_produce(self, msgs: list[Mapping[str, Any]]) -> None:
        try:
            for msg in msgs:
                self.kafka_handler.produce(key=str(msg["id"]), value=msg)
            self.flush()
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Error : %s", error)

    def flush(self) -> None:
        self.kafka_handler.flush()
