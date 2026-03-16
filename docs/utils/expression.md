# Expression Utility

## Purpose
Evaluate postfix (Reverse Polish Notation) expressions used by input/state-machine conditions.

## Key Classes and Functions
- `data_service_sdk.utils.expression.Expression.evaluate(equation, variable_value_mapping)`
- Supported operators: `+`, `-`, `*`, `/`, `>`, `<`, `<=`, `>=`

## Minimal Configuration Example
```python
from data_service_sdk.utils.expression import Expression

result_sum = Expression.evaluate(["a", "b", "+"], {"a": 10, "b": 5})
result_cmp = Expression.evaluate(["temperature", "limit", ">"], {"temperature": 82, "limit": 80})
```

## Known Constraints and Edge Cases
- Tokens not in `operations` are treated as variable names and must exist in the mapping.
- Invalid postfix equations can raise stack/index errors (for example missing operands).
- Result type depends on operator and operand types (`int`, `float`, `bool`).

## Related Links
- [Input Handlers](../handlers/input.md)
- [State Machine Examples](../handlers/state-machine-examples.md)
- [Typed Contracts](../reference/types.md)
- [Documentation Index](../index.md)
