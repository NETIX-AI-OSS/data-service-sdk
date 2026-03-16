from __future__ import annotations

from typing import Dict, Union

import pytest
from framework.handlers.input.state_machine import StateMachineInputController
from framework.types import StateMachineConfig
from framework.utils.expression import Expression


def test_expression_evaluate_all_ops() -> None:
    mapping: Dict[str, Union[int, float, bool]] = {"a": 5, "b": 2, "c": 5}
    assert Expression.evaluate(["a", "b", "-"], mapping) == 3
    mapping = {"x": 2, "y": 8}
    assert Expression.evaluate(["x", "y", "<"], mapping) is True
    mapping = {"x": 3, "y": 3}
    assert Expression.evaluate(["x", "y", "<="], mapping) is True


def test_state_machine_process_true_path() -> None:
    config: StateMachineConfig = {
        "states": ["s1", "s2"],
        "transitions": {
            "ts1_ts2": {
                "trigger": "ts1_ts2",
                "source": "s1",
                "dest": "s2",
                "conditions_config": [{"equation": ["x", "y", "<"]}],
            }
        },
    }
    controller = StateMachineInputController(config, initial_state="s1")
    result = controller.process({"x": 1, "y": 2})
    assert result is True
    assert controller.state == "s2"


def test_state_machine_process_false_path() -> None:
    config: StateMachineConfig = {
        "states": ["s1", "s2"],
        "transitions": {
            "ts1_ts2": {
                "trigger": "ts1_ts2",
                "source": "s1",
                "dest": "s2",
                "conditions_config": [{"equation": ["x", "y", "<"]}],
            }
        },
    }
    controller = StateMachineInputController(config, initial_state="s1")
    result = controller.process({"x": 3, "y": 1})
    assert result is False
    assert controller.state == "s1"


def test_state_machine_handles_missing_transition(caplog: pytest.LogCaptureFixture) -> None:
    config: StateMachineConfig = {
        "states": ["s1", "s2"],
        "transitions": {
            "foo": {
                "trigger": "foo",
                "source": "s1",
                "dest": "s2",
                "conditions_config": [{"equation": ["x", "y", "<"]}],
            }
        },
    }
    controller = StateMachineInputController(config, initial_state="s1")

    with caplog.at_level("ERROR"):
        result = controller.process({})

    assert result is False
    assert any("error" in record.levelname.lower() or record.levelname == "ERROR" for record in caplog.records)
