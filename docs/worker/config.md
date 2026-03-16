# Worker Config Loading

## Purpose
Load worker JSON configuration files from mounted config paths.

## Key Classes and Functions
- `data_service_sdk.worker.utils.config_handler.ConfigHandler.get_config(config)`
- `data_service_sdk.worker.utils.config_handler.ConfigHandler.get_example_config(config)`

## Minimal Configuration Example
```python
from data_service_sdk.worker.utils.config_handler import ConfigHandler

runtime_config = ConfigHandler.get_config("config.json")
example_config = ConfigHandler.get_example_config("sample_config.json")
```

Runtime paths used by the implementation:
- `/etc/config/<config>` for active worker config.
- `/home/appuser/code/worker/utils/configs/examples/<config>` for example files.

## Known Constraints and Edge Cases
- Both methods expect valid JSON content; malformed JSON raises decode errors.
- Example path is environment-specific and may not exist in all local setups.

## Related Links
- [Worker Deployment](deployment.md)
- [Getting Started Overview](../getting-started/overview.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
