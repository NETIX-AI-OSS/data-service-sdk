# State Machine Examples

## Purpose
Provide cleaned, practical examples for building input state machines and evaluating transitions.

## Key Classes and Functions
- `StateMachineInputController.__init__(config, initial_state)`
- `StateMachineInputController.process(variable_value_mapping)`
- `StateMachineInputController.tick(meta, variable_value_mapping)`

## Minimal Configuration Example
```python
from data_service_sdk.handlers.input.state_machine import StateMachineInputController

config = {
    "states": ["init", "high", "highhigh"],
    "transitions": {
        "tinit_thigh": {
            "trigger": "tinit_thigh",
            "source": "init",
            "dest": "high",
            "conditions_config": [{"equation": ["value", "limit_high", ">"]}],
        },
        "thigh_tinit": {
            "trigger": "thigh_tinit",
            "source": "high",
            "dest": "init",
            "conditions_config": [{"equation": ["value", "reset_high", "<"]}],
        },
        "thigh_thighhigh": {
            "trigger": "thigh_thighhigh",
            "source": "high",
            "dest": "highhigh",
            "conditions_config": [{"equation": ["value", "limit_highhigh", ">"]}],
        },
    },
}

controller = StateMachineInputController(config=config, initial_state="init")

values = {
    "value": 95,
    "limit_high": 80,
    "reset_high": 75,
    "limit_highhigh": 90,
}

result = controller.process(values)
print(controller.state, result)
```

## Known Constraints and Edge Cases
- Equation tokens must be valid postfix expressions and map to keys in `variable_value_mapping`.
- A transition condition is considered satisfied when expression evaluation returns a truthy value.
- Only transitions reachable from current state are attempted in `process()`.
- Large vectorized inputs from legacy samples are not required by controller APIs; pass scalar values per evaluation.

## Related Links
- [Input Handlers](input.md)
- [Expression Utility](../utils/expression.md)
- [Typed Contracts](../reference/types.md)
- [Documentation Index](../index.md)
