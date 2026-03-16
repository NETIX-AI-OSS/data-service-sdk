from __future__ import annotations

import importlib
from typing import cast

from framework import types as framework_types
from framework.utils import timeseries
from framework.worker.deployment import deployment_handler


def test_framework_types_protocol_method_bodies() -> None:
    producer = cast(framework_types.OutputProducer, object())
    producer_factory = cast(framework_types.OutputProducerFactory, object())
    row_mapping = cast(framework_types.RowMapping, object())

    assert framework_types.OutputProducer.produce(producer, {"k": 1}) is None
    assert framework_types.OutputProducerFactory.__call__(producer_factory, {"k": "v"}) is None
    assert framework_types.RowMapping.__getitem__(row_mapping, "key") is None


def test_timeseries_protocol_method_bodies() -> None:
    client = cast(timeseries.RedisTimeseriesClient, object())
    wrapper = cast(timeseries.RedisTimeseriesWrapper, object())

    assert timeseries.RedisTimeseriesClient.create(client, "k", 1000, {"id": "1"}, "BLOCK") is None
    assert timeseries.RedisTimeseriesClient.add(client, "k", 1, 1.0) is None
    assert timeseries.RedisTimeseriesClient.get(client, "k") is None
    assert timeseries.RedisTimeseriesClient.revrange(client, "k", "-", "+", count=1) is None
    assert timeseries.RedisTimeseriesWrapper.ts(wrapper) is None


def test_worker_handler_protocol_method_bodies() -> None:
    output_destinations = cast(deployment_handler.WorkerHandler.OutputDestinationManager, object())
    assert deployment_handler.WorkerHandler.OutputDestinationManager.all(output_destinations) is None


def test_public_package_namespace_aliases_framework_modules() -> None:
    public_types = importlib.import_module("data_service_sdk.types")
    public_factory = importlib.import_module("data_service_sdk.handlers.output.factory")
    framework_factory = importlib.import_module("framework.handlers.output.factory")

    assert public_types is framework_types
    assert public_factory is framework_factory
