from __future__ import annotations

import logging
from typing import Dict, List, Mapping, Optional, Union
from transitions import Machine

from framework.utils.expression import Expression
from framework.types import StateMachineConfig

logger = logging.getLogger(__name__)


class StateMachineInputController:
    state: str

    def __init__(self, config: StateMachineConfig, initial_state: str) -> None:
        self.result: Optional[Union[int, float, bool]] = None
        self.config: StateMachineConfig = config
        transitions_dict = config["transitions"]
        transitions = []

        self.source_destination_mapping: Dict[str, List[str]] = {}
        for source in config["states"]:
            self.source_destination_mapping[source] = []

        for transition_key, _ in config["transitions"].items():
            transition = transitions_dict[transition_key]
            logger.debug(transition)

            transition_dict = {
                "trigger": transition["trigger"],
                "source": transition["source"],
                "dest": transition["dest"],
                "conditions": "tick",
                # 'after': 'after'# on_exit=['say_goodbye']
            }

            transitions.append(transition_dict)

            self.source_destination_mapping[transition["source"]].append(transition["dest"])

        states = config["states"]
        machine = Machine(model=self, states=states, transitions=transitions, initial=initial_state)
        logger.debug(machine.states)
        self.state = initial_state

    def tick(self, meta: str, variable_value_mapping: Mapping[str, Union[int, float, bool]]) -> Union[int, float, bool]:
        trigger_block = self.config["transitions"][meta]
        logger.debug("variable_value_mapping")
        logger.debug(variable_value_mapping)
        logger.debug(trigger_block["conditions_config"][0]["equation"])
        result = Expression.evaluate(trigger_block["conditions_config"][0]["equation"], variable_value_mapping)
        logger.debug("-----------")
        logger.debug(result)
        self.result = result
        return result

    # def after(self, meta, variable_value_mapping):
    # 	trigger_block = self.config['transitions'][meta]
    # 	result = evaluate(trigger_block['output_config']
    # 	                  [0]['equation'], variable_value_mapping)
    # 	return True

    def process(self, variable_value_mapping: Mapping[str, Union[int, float, bool]]) -> Union[int, float, bool]:
        logger.debug(self.state)
        logger.debug(self.source_destination_mapping)

        for destination in self.source_destination_mapping[self.state]:
            transition = f"t{self.state}_t{destination}"
            try:
                self.result = (getattr(self, transition))(transition, variable_value_mapping)
                if self.result:
                    return self.result
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(str(exc))
        return False
