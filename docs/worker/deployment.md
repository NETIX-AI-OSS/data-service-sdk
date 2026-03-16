# Worker Deployment

## Purpose
Describe how worker runtime config is composed and deployed to Kubernetes using ConfigMap + Deployment abstractions.

## Key Classes and Functions
- `data_service_sdk.worker.deployment.deployment_handler.WorkerHandler.build_full_config(...)`
- `WorkerHandler.deploy(...)`, `deploy_config(...)`, `delete(...)`, `update(...)`
- Naming helpers: `get_worker_name(...)`, `get_config_map_name(...)`

## Minimal Configuration Example
```python
from data_service_sdk.worker.deployment.deployment_handler import WorkerHandler

# worker is expected to expose protocol-compatible attributes:
# id, input_type.input_id_internal, output_destinations.all(), config, full_config,
# image, command, namespace, type(image/command/namespace)

full_config = WorkerHandler.build_full_config(worker)
WorkerHandler.deploy(worker, worker_base_name="worker", update=False, organization_id=1)
```

Resulting full config shape includes:
- `input_controller_type` from `worker.input_type.input_id_internal`
- `output_config.output_handlers_config` from `worker.output_destinations.all()` when output is enabled

## Known Constraints and Edge Cases
- Deployment wrappers rely on `k8s_utils` and Kubernetes API availability.
- `WorkerHandler.deploy(...)` fills missing image/command/namespace from `worker.type`.
- Delete flow retries transient API errors and treats `404` as already deleted.
- Config payload is persisted as JSON under `config.json` in ConfigMap.

## Related Links
- [Worker Config Loading](config.md)
- [Worker State and Offset](state-and-offset.md)
- [Typed Contracts](../reference/types.md)
- [Coverage Map](../testing/coverage.md)
- [Documentation Index](../index.md)
