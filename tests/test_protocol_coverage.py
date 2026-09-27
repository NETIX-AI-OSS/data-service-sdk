from __future__ import annotations

import importlib

from framework import types as framework_types


def test_public_package_namespace_aliases_framework_modules() -> None:
    public_types = importlib.import_module("data_service_sdk.types")
    public_factory = importlib.import_module("data_service_sdk.handlers.output.factory")
    framework_factory = importlib.import_module("framework.handlers.output.factory")

    assert public_types is framework_types
    assert public_factory is framework_factory
