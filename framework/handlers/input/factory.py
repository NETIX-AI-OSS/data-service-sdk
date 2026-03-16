from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Type, Union, cast

from .expression import ExpressionInputController
from .input_controller_type import INPUT_CONTROLLER_TYPE
from .state_machine import StateMachineInputController

logger = logging.getLogger(__name__)


class Factory:
    @staticmethod
    def get_input_controller(
        input_controller_type: int, config: Dict[str, Any]
    ) -> Optional[Union[ExpressionInputController, Type[StateMachineInputController]]]:
        logger.debug("Factory")
        input_controller: Optional[Union[ExpressionInputController, Type[StateMachineInputController]]] = None
        if input_controller_type == INPUT_CONTROLLER_TYPE["expression"]:
            input_controller = ExpressionInputController(config=cast(Any, config))
            logger.debug("ExpressionInputController created")

        elif input_controller_type == INPUT_CONTROLLER_TYPE["state_machine"]:
            input_controller = StateMachineInputController
            logger.debug("StateMachineInputController created")

        return input_controller
