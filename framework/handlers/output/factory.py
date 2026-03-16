from __future__ import annotations

import logging
from typing import Any, Dict, Optional, cast

from framework.handlers.output import kafka_controller, tsdb_controller, api_controller
from framework.handlers.output.type import OUTPUT_CONTROLLER_TYPE
from framework.handlers.utils.mqtt_handler import MqttHandler
from framework.types import ApiOutputConfig, KafkaOutputConfig, MqttConfig, OutputProducer, TsdbOutputConfig

logger = logging.getLogger(__name__)


class Factory:
    @staticmethod
    def get_output_controller(controller_type: str, config: Dict[str, Any]) -> Optional[OutputProducer]:
        logger.debug("Factory")
        logger.debug(controller_type)

        controller: Optional[OutputProducer] = None
        if controller_type == OUTPUT_CONTROLLER_TYPE["kafka"]:
            controller = kafka_controller.KafkaOutputController(config=cast(KafkaOutputConfig, config))
            logger.debug("KafkaOutput Controller created")

        elif controller_type == OUTPUT_CONTROLLER_TYPE["tsdb"]:
            controller = tsdb_controller.TsdbOutputController(config=cast(TsdbOutputConfig, config))
            logger.debug("TsdbOutput Controller created")

        elif controller_type == OUTPUT_CONTROLLER_TYPE["api"]:
            controller = api_controller.ApiOutputController(config=cast(ApiOutputConfig, config))
            logger.debug("ApiOutput Controller created")

        elif controller_type == OUTPUT_CONTROLLER_TYPE["mqtt"]:
            mqtt_config = cast(MqttConfig, config)
            try:
                controller = MqttHandler()
                controller.init_producer(
                    mqtt_config["topic"],
                    mqtt_config["host"],
                    mqtt_config["user"],
                    mqtt_config["password"],
                )
                logger.debug("Mqtt publisher created")
            except KeyError as e:
                logger.error("Missing required MQTT configuration field: %s", e)
                raise ValueError(f"MqttConfig missing required field: {e}") from e

        return controller
