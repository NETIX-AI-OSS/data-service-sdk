from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from framework.handlers.subscription.type import SUBSCRIPTION_TYPE
from framework.handlers.utils import kafka_handler, mqtt_handler

logger = logging.getLogger(__name__)


class Factory:
    @staticmethod
    def get_subscription_controller(
        controller_type: str, config: Dict[str, Any]
    ) -> Optional[Union[kafka_handler.KafkaHandler, mqtt_handler.MqttHandler]]:
        logger.debug("Factory")
        logger.debug(controller_type)

        controller: Optional[Union[kafka_handler.KafkaHandler, mqtt_handler.MqttHandler]] = None
        if controller_type == SUBSCRIPTION_TYPE["kafka"]:
            controller = kafka_handler.KafkaHandler()
            controller.init_consumer(
                config.get("kafka_topic_subscribe", ""),
                config.get("kafka_bootstrap_servers", ""),
                config.get("kafka_ts_group_id", ""),
                config.get("kafka_ts_offset_reset", "earliest"),
                config.get("kafka_ts_auto_commit", True),
                config.get("polling_interval_secs", 30),
            )
            logger.debug("Kafka subscription Controller created")

        elif controller_type == SUBSCRIPTION_TYPE["mqtt"]:
            controller = mqtt_handler.MqttHandler()
            controller.init_consumer(
                config.get("topic", ""),
                config.get("host", ""),
                config.get("user", ""),
                config.get("password", ""),
            )
            logger.debug("Mqtt subscription created")

        # elif controller_type == SUBSCRIPTION_TYPE['api']:
        #     controller = api_controller.ApiOutputController(config=config)
        #     logger.debug("ApiOutput Controller created")

        return controller
