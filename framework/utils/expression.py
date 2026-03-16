from __future__ import annotations

import logging
from typing import Dict, List, Mapping, Union

logger = logging.getLogger(__name__)


class Expression:
    operations = {
        "+": lambda x, y: x + y,
        "-": lambda x, y: x - y,
        "*": lambda x, y: x * y,
        "/": lambda x, y: x / y,
        ">": lambda x, y: x > y,
        "<": lambda x, y: x < y,
        "<=": lambda x, y: x <= y,
        ">=": lambda x, y: x >= y,
    }

    @staticmethod
    def evaluate(
        equation: List[str], variable_value_mapping: Mapping[str, Union[int, float, bool]]
    ) -> Union[int, float, bool]:
        values_mapping: Dict[str, Union[int, float, bool]] = dict(variable_value_mapping)
        logger.debug("values_mapping---------")
        logger.debug(values_mapping)

        values: List[str] = []
        for token in equation:  # loop for checking each character one by one
            if token not in Expression.operations:
                values.append(token)
            else:
                x_name = values.pop()
                y_name = values.pop()
                result_name = x_name + y_name + token

                x_values = values_mapping[x_name]
                y_values = values_mapping[y_name]

                result = Expression.operations[token](y_values, x_values)
                logger.debug(result)
                values_mapping[result_name] = result
                values.append(result_name)

        return values_mapping[values[0]]
