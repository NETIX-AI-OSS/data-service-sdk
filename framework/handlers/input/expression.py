from __future__ import annotations

import logging
from collections.abc import Mapping

from framework.types import ExpressionConfig

# from framework.input.utils.expression import evaluate
from framework.utils.expression import Expression

logger = logging.getLogger(__name__)


class ExpressionInputController:
    def __init__(self, config: ExpressionConfig) -> None:
        self.config: ExpressionConfig = config

    def process(self, variable_value_mapping: Mapping[str, int | float | bool]) -> int | float | bool:
        # trigger_block = list(filter(lambda tirgger: tirgger['trigger'] == meta, config['transitions']))[0]
        result = Expression.evaluate(self.config["equation"], variable_value_mapping)
        logger.debug("-----------")
        logger.debug(result)
        return result
