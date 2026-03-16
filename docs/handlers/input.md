# Input Handlers

## Purpose
Define and execute computation input logic using either postfix expressions or transition-driven state machines.

## Key Classes and Functions
- `data_service_sdk.handlers.input.factory.Factory.get_input_controller(...)`
- `data_service_sdk.handlers.input.expression.ExpressionInputController.process(...)`
- `data_service_sdk.handlers.input.state_machine.StateMachineInputController.process(...)`
- `data_service_sdk.handlers.input.state_machine.StateMachineInputController.tick(...)`

## Minimal Configuration Example
Expression controller:

```python
from data_service_sdk.handlers.input.expression import ExpressionInputController

controller = ExpressionInputController(
    {
        "equation": ["temperature", "threshold", ">"],
    }
)

result = controller.process({"temperature": 84, "threshold": 80})
```

State machine controller:

```python
from data_service_sdk.handlers.input.state_machine import StateMachineInputController

config = {
    "states": ["normal", "high"],
    "transitions": {
        "tnormal_thigh": {
            "trigger": "tnormal_thigh",
            "source": "normal",
            "dest": "high",
            "conditions_config": [{"equation": ["temp", "limit", ">"]}],
        }
    },
}

controller = StateMachineInputController(config=config, initial_state="normal")
outcome = controller.process({"temp": 90, "limit": 80})
```

## Known Constraints and Edge Cases
- Transition methods are resolved dynamically by naming convention `t{source}_t{dest}`.
- `process()` returns `False` when no transition condition succeeds.
- Transition evaluation uses postfix token evaluation from `data_service_sdk.utils.expression.Expression`.
- Invalid/missing transition methods are caught and logged as errors.

## Related Links
- [State Machine Examples](state-machine-examples.md)
- [Expression Utility](../utils/expression.md)
- [Typed Contracts](../reference/types.md)
- [Documentation Index](../index.md)
