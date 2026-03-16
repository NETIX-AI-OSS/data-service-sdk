from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping

from framework.handlers.output.factory import Factory
from framework.types import OutputProducer

logger = logging.getLogger(__name__)


@dataclass
class OutputManager:
    output_controller_list: List[OutputProducer] = field(default_factory=list)

    def get_controller(self, config: Mapping[str, Any]) -> OutputProducer:
        controller = Factory.get_output_controller(config["type"], dict(config))
        if controller is None:
            raise ValueError(f"Unknown output controller type: {config['type']}")
        self.output_controller_list.append(controller)
        return controller

    def init_manager(self, output_handlers_config: List[Dict[str, Any]]) -> None:
        for output_controller in output_handlers_config:
            logger.debug(output_controller)
            _ = self.get_controller(output_controller["config"])

    # def remove_controller(self, output_type):
    #     try:
    #         del self.output_controller_map[output_type]
    #         return True
    #     except KeyError as exc:
    #         logger.error("No such key : %s", exc)
    #     return False

    def produce(self, msg: Any) -> None:
        for output_controller in self.output_controller_list:
            output_controller.produce(msg)

    def batch_produce(self, msgs: List[Any]) -> None:
        for output_controller in self.output_controller_list:
            for msg in msgs:
                output_controller.produce(msg)
