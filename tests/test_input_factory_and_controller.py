from __future__ import annotations

# pylint: disable=duplicate-code

from framework.handlers.input import expression as expression_controller
from framework.handlers.input.factory import Factory
from framework.handlers.input.input_controller_type import INPUT_CONTROLLER_TYPE
from framework.handlers.input.state_machine import StateMachineInputController


def test_expression_input_controller() -> None:
    controller = expression_controller.ExpressionInputController({"equation": ["a", "b", "+"]})
    result = controller.process({"a": 1, "b": 2})

    assert result == 3


def test_input_factory_expression() -> None:
    controller = Factory.get_input_controller(INPUT_CONTROLLER_TYPE["expression"], {"equation": ["a", "b", "+"]})
    assert isinstance(controller, expression_controller.ExpressionInputController)


def test_input_factory_state_machine() -> None:
    controller = Factory.get_input_controller(INPUT_CONTROLLER_TYPE["state_machine"], {})
    assert controller is StateMachineInputController


def test_input_factory_unknown() -> None:
    controller = Factory.get_input_controller(0, {})
    assert controller is None
