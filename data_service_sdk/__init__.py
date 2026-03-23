from __future__ import annotations

import importlib
import sys
from types import ModuleType

__version__ = "1.1.0"

_MODULE_ALIASES = {
    "data_service_sdk.types": "framework.types",
    "data_service_sdk.handlers": "framework.handlers",
    "data_service_sdk.handlers.input": "framework.handlers.input",
    "data_service_sdk.handlers.input.expression": "framework.handlers.input.expression",
    "data_service_sdk.handlers.input.factory": "framework.handlers.input.factory",
    "data_service_sdk.handlers.input.input_controller_type": "framework.handlers.input.input_controller_type",
    "data_service_sdk.handlers.input.state_machine": "framework.handlers.input.state_machine",
    "data_service_sdk.handlers.output": "framework.handlers.output",
    "data_service_sdk.handlers.output.api_controller": "framework.handlers.output.api_controller",
    "data_service_sdk.handlers.output.factory": "framework.handlers.output.factory",
    "data_service_sdk.handlers.output.kafka_controller": "framework.handlers.output.kafka_controller",
    "data_service_sdk.handlers.output.manager": "framework.handlers.output.manager",
    "data_service_sdk.handlers.output.tsdb_controller": "framework.handlers.output.tsdb_controller",
    "data_service_sdk.handlers.output.type": "framework.handlers.output.type",
    "data_service_sdk.handlers.subscription": "framework.handlers.subscription",
    "data_service_sdk.handlers.subscription.factory": "framework.handlers.subscription.factory",
    "data_service_sdk.handlers.subscription.manager": "framework.handlers.subscription.manager",
    "data_service_sdk.handlers.subscription.type": "framework.handlers.subscription.type",
    "data_service_sdk.handlers.utils": "framework.handlers.utils",
    "data_service_sdk.handlers.utils.db_handler": "framework.handlers.utils.db_handler",
    "data_service_sdk.handlers.utils.kafka_handler": "framework.handlers.utils.kafka_handler",
    "data_service_sdk.handlers.utils.mqtt_handler": "framework.handlers.utils.mqtt_handler",
    "data_service_sdk.utils": "framework.utils",
    "data_service_sdk.utils.expression": "framework.utils.expression",
    "data_service_sdk.utils.redis_timeseries": "framework.utils.redis_timeseries",
    "data_service_sdk.utils.timeseries": "framework.utils.timeseries",
    "data_service_sdk.utils.timeseries_manager": "framework.utils.timeseries_manager",
    "data_service_sdk.worker": "framework.worker",
    "data_service_sdk.worker.base_worker": "framework.worker.base_worker",
    "data_service_sdk.worker.deployment": "framework.worker.deployment",
    "data_service_sdk.worker.deployment.deployment_handler": "framework.worker.deployment.deployment_handler",
    "data_service_sdk.worker.state_handler": "framework.worker.state_handler",
    "data_service_sdk.worker.state_handler.state_handler": "framework.worker.state_handler.state_handler",
    "data_service_sdk.worker.utils": "framework.worker.utils",
    "data_service_sdk.worker.utils.config_handler": "framework.worker.utils.config_handler",
}


def _alias_module(public_name: str, internal_name: str) -> ModuleType:
    module = importlib.import_module(internal_name)
    sys.modules.setdefault(public_name, module)
    return module


types = _alias_module("data_service_sdk.types", "framework.types")
handlers = _alias_module("data_service_sdk.handlers", "framework.handlers")
utils = _alias_module("data_service_sdk.utils", "framework.utils")
worker = _alias_module("data_service_sdk.worker", "framework.worker")

for _pub_name, _int_name in _MODULE_ALIASES.items():
    _alias_module(_pub_name, _int_name)

__all__ = ["handlers", "types", "utils", "worker", "__version__"]
