from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, ClassVar

logger = logging.getLogger(__name__)


class Expression:
    operations: ClassVar[dict[str, Any]] = {
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
    def evaluate(equation: list[str], variable_value_mapping: Mapping[str, int | float | bool]) -> int | float | bool:
        values_mapping: dict[str, int | float | bool] = dict(variable_value_mapping)
        logger.debug("values_mapping---------")
        logger.debug(values_mapping)

        values: list[str] = []
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
