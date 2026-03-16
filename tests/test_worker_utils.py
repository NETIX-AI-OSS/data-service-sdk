from __future__ import annotations

import io
import json
from typing import Any, Dict, List, cast

import pytest

from framework.worker import base_worker
from framework.worker.deployment import deployment_handler
from framework.worker.state_handler import state_handler
from framework.worker.utils import config_handler


class DummyTimeseries:
    def __init__(self) -> None:
        self.values: List[Any] = []

    def get_last_n(self, _count: int) -> List[Any]:
        return self.values

    def add(self, value: Any) -> None:
        self.values.append((0, value))


class DummyTimeseriesManager:
    def __init__(self) -> None:
        self.created: Dict[str, DummyTimeseries] = {}

    def get_timeseries_create_if_not_present(self, ts_id: str) -> DummyTimeseries:
        if ts_id not in self.created:
            self.created[ts_id] = DummyTimeseries()
        return self.created[ts_id]


def test_state_handler_get_set_state(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = DummyTimeseriesManager()
    monkeypatch.setattr(state_handler, "TimeseriesManger", lambda: manager)
    state_handler.StateHandler.timeseries_manager = cast(Any, manager)

    handler = state_handler.StateHandler()
    handler.set_state("worker", "state", 123)

    assert handler.get_state("worker", "state", default=0) == 123


def test_state_handler_get_state_default(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = DummyTimeseriesManager()
    monkeypatch.setattr(state_handler, "TimeseriesManger", lambda: manager)
    state_handler.StateHandler.timeseries_manager = cast(Any, manager)

    handler = state_handler.StateHandler()

    assert handler.get_state("worker", "state", default=0) == 0


def test_worker_offset_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = DummyTimeseriesManager()
    monkeypatch.setattr(state_handler, "TimeseriesManger", lambda: manager)
    state_handler.StateHandler.timeseries_manager = cast(Any, manager)

    state_handler.WorkerOffsetHandler.state_handler = state_handler.StateHandler()
    offset_handler = state_handler.WorkerOffsetHandler()
    now = offset_handler.get_offset("worker", retention_ts_sec=60)
    offset_handler.set_offset("worker", now)

    ts = manager.created["worker/worker_offset_timestamp"]
    assert ts.values


def test_config_handler_reads_config(monkeypatch: pytest.MonkeyPatch) -> None:
    data = {"a": 1}

    def fake_open(*_args: Any, **_kwargs: Any) -> io.StringIO:
        return io.StringIO(json.dumps(data))

    monkeypatch.setattr("builtins.open", fake_open)

    assert config_handler.ConfigHandler.get_config("config.json") == data
    assert config_handler.ConfigHandler.get_example_config("config.json") == data


def test_base_worker_sets_config() -> None:
    class DemoWorker(base_worker.BaseWorker):
        def loop(self) -> None:
            return None

    worker = DemoWorker()
    worker.set_config({"a": 1})
    assert worker.config == {"a": 1}


def test_base_worker_loop_raises() -> None:
    with pytest.raises(NotImplementedError):
        base_worker.BaseWorker.loop(object())  # type: ignore[arg-type]


def test_worker_handler_builds_config_and_names() -> None:
    class DummyInputType:
        input_id_internal = "input"

    class DummyOutput:
        def __init__(self, config: Dict[str, Any]) -> None:
            self.config = config

    class DummyOutputs:
        def all(self) -> List[DummyOutput]:
            return [DummyOutput({"k": "v"})]

    class DummyType:
        image = "img"
        command = "cmd"
        namespace = "ns"

    class DummyWorker:
        id = 1
        input_type = DummyInputType()
        output_destinations = DummyOutputs()
        config = {"output": True, "output_config": {"output_handlers_config": []}}
        full_config: Dict[str, Any] = {}
        image = ""
        command = ""
        namespace = ""
        type = DummyType()

    worker = DummyWorker()
    full_config = deployment_handler.WorkerHandler.build_full_config(cast(Any, worker))

    assert full_config["input_controller_type"] == "input"
    assert full_config["output_config"]["output_handlers_config"][0] == {"k": "v"}
    assert deployment_handler.WorkerHandler.get_worker_name(cast(Any, worker)) == "worker-1"
    assert deployment_handler.WorkerHandler.get_config_map_name(cast(Any, worker)) == "worker-1-config"


def test_worker_handler_deploy_and_config(monkeypatch: pytest.MonkeyPatch) -> None:
    deployments: List[Any] = []
    configmaps: List[Any] = []
    delete_events: List[str] = []

    class DummyApiException(Exception):
        def __init__(self, body: str) -> None:
            super().__init__(body)
            self.body = body

    class DummyDeployment:
        def __init__(self, app_name: str, namespace: str, sa_enabled: bool, config_file: str) -> None:
            deployments.append((app_name, namespace, sa_enabled, config_file))
            self.delete_calls = 0

        def add_env_from(self, _env_from: str) -> None:
            return None

        def create(self, **_kwargs: Any) -> None:
            return None

        def update(self, **_kwargs: Any) -> None:
            return None

        def delete(self) -> None:
            self.delete_calls += 1
            delete_events.append(f"deployment:{self.delete_calls}")
            if self.delete_calls == 1:
                raise DummyApiException(json.dumps({"code": 500}))

    class DummyConfigMap:
        def __init__(self, name: str, sa_enabled: bool, namespace: str, config_file: str) -> None:
            configmaps.append((name, sa_enabled, namespace, config_file))
            self.delete_calls = 0

        def create(self, filename: str, content: str) -> None:
            _ = (filename, content)

        def update(self, filename: str, content: str) -> None:
            _ = (filename, content)

        def delete(self) -> None:
            self.delete_calls += 1
            delete_events.append(f"config:{self.delete_calls}")
            if self.delete_calls == 1:
                raise DummyApiException(json.dumps({"code": 500}))
            raise DummyApiException(json.dumps({"code": 404}))

    monkeypatch.setattr(deployment_handler, "ApiException", DummyApiException)
    monkeypatch.setattr(deployment_handler, "Deployment", DummyDeployment)
    monkeypatch.setattr(deployment_handler, "ConfigMap", DummyConfigMap)

    class DummyInputType:
        input_id_internal = "input"

    class DummyOutputs:
        def all(self) -> List[Any]:
            return []

    class DummyType:
        image = "img"
        command = "cmd"
        namespace = "ns"

    class DummyWorker:
        id = 2
        input_type = DummyInputType()
        output_destinations = DummyOutputs()
        config = {"output": False, "output_config": {"output_handlers_config": []}}
        full_config: Dict[str, Any] = {}
        image = ""
        command = ""
        namespace = ""
        type = DummyType()

    worker = DummyWorker()

    deployment_handler.WorkerHandler.deploy(cast(Any, worker))
    deployment_handler.WorkerHandler.deploy(
        cast(Any, worker), deployment_handler.WorkerHandler.DeployOptions(update=True)
    )
    deployment_handler.WorkerHandler.deploy_config(cast(Any, worker), update=True)
    deployment_handler.WorkerHandler.delete(cast(Any, worker))
    deployment_handler.WorkerHandler.update(cast(Any, worker))
    deployment_handler.WorkerHandler.check(cast(Any, worker))

    assert deployments
    assert configmaps
    assert delete_events.count("deployment:1") == 2
    assert delete_events.count("deployment:2") == 2
    assert delete_events.count("config:1") == 2
    assert delete_events.count("config:2") == 2


def test_worker_handler_delete_config_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyConfigMap:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = (args, kwargs)

        def delete(self) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(deployment_handler, "ConfigMap", DummyConfigMap)

    class DummyWorker:
        id = 3
        namespace = "ns"

    with pytest.raises(RuntimeError, match="boom"):
        deployment_handler.WorkerHandler.delete_config(cast(Any, DummyWorker()))


def test_worker_handler_delete_config_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyConfigMap:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = (args, kwargs)

        def delete(self) -> None:
            return None

    monkeypatch.setattr(deployment_handler, "ConfigMap", DummyConfigMap)

    class DummyWorker:
        id = 4
        namespace = "ns"

    deployment_handler.WorkerHandler.delete_config(cast(Any, DummyWorker()))


def test_worker_handler_delete_config_handles_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyApiException(Exception):
        def __init__(self, body: str) -> None:
            super().__init__(body)
            self.body = body

    class DummyConfigMap:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = (args, kwargs)

        def delete(self) -> None:
            raise DummyApiException(json.dumps({"code": 404}))

    monkeypatch.setattr(deployment_handler, "ApiException", DummyApiException)
    monkeypatch.setattr(deployment_handler, "ConfigMap", DummyConfigMap)

    class DummyWorker:
        id = 5
        namespace = "ns"

    deployment_handler.WorkerHandler.delete_config(cast(Any, DummyWorker()))


def test_worker_handler_delete_config_raises_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyApiException(Exception):
        def __init__(self, body: str) -> None:
            super().__init__(body)
            self.body = body

    class DummyConfigMap:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = (args, kwargs)

        def delete(self) -> None:
            raise DummyApiException(json.dumps({"code": 500}))

    monkeypatch.setattr(deployment_handler, "ApiException", DummyApiException)
    monkeypatch.setattr(deployment_handler, "ConfigMap", DummyConfigMap)

    class DummyWorker:
        id = 6
        namespace = "ns"

    with pytest.raises(DummyApiException):
        deployment_handler.WorkerHandler.delete_config(cast(Any, DummyWorker()))


def test_worker_handler_is_not_found_error_handles_invalid_body() -> None:
    class DummyApiException(Exception):
        def __init__(self, body: str | None) -> None:
            super().__init__(body)
            self.body = body

    is_not_found = deployment_handler.WorkerHandler._is_not_found_error  # pylint: disable=protected-access
    assert is_not_found(DummyApiException(None)) is False
    assert is_not_found(DummyApiException("not-json")) is False
