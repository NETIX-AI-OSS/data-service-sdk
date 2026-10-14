from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple, cast

import pytest
import urllib3
from kubernetes.client import ApiException

from framework.worker.deployment import deployment_handler

WorkerHandler = deployment_handler.WorkerHandler
CONFIGMAPS = "/api/v1/namespaces/workers/configmaps"
DEPLOYMENTS = "/apis/apps/v1/namespaces/workers/deployments"


def _stub_http(monkeypatch: pytest.MonkeyPatch, status: int = 200) -> List[Tuple[str, str, Any]]:
    sent: List[Tuple[str, str, Any]] = []

    def fake_request(_pool: Any, method: str, url: str, *_args: Any, **kwargs: Any) -> urllib3.HTTPResponse:
        body = json.loads(kwargs["body"]) if kwargs.get("body") else None
        sent.append((method, urllib3.util.parse_url(url).path or "", body))
        reply: Any = body if method in ("POST", "PUT", "PATCH") else {}
        if status >= 400:
            reply = {"kind": "Status", "code": status}
        return urllib3.HTTPResponse(
            body=json.dumps(reply).encode(), status=status, headers={"Content-Type": "application/json"}
        )

    monkeypatch.setattr(urllib3.PoolManager, "request", fake_request)
    monkeypatch.setattr("k8s_utils.base.config.load_kube_config", lambda **_kwargs: None)
    return sent


def _worker() -> Any:
    class InputType:
        input_id_internal = "input"

    class Outputs:
        def all(self) -> List[Any]:
            return []

    class WorkerType:
        image = "registry/worker:1"
        command = "python -m worker"
        namespace = "workers"

    class Worker:
        id = 7
        input_type = InputType()
        output_destinations = Outputs()
        config: Dict[str, Any] = {"output": False}
        full_config: Dict[str, Any] = {}
        image = ""
        command = ""
        namespace = ""
        type = WorkerType()

    return cast(Any, Worker())


def test_deploy_sends_valid_create_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = _stub_http(monkeypatch)
    options = WorkerHandler.DeployOptions(env_from="data-configmap", image_pull_secrets="regcred", organization_id=9)
    WorkerHandler.deploy(_worker(), options)

    assert [(method, path) for method, path, _ in sent] == [("POST", CONFIGMAPS), ("POST", DEPLOYMENTS)]
    configmap, deployment = sent[0][2], sent[1][2]
    assert configmap["metadata"] == {"name": "worker-7-config", "namespace": "workers"}
    assert json.loads(configmap["data"]["config.json"]) == {"output": False, "input_controller_type": "input"}
    pod = deployment["spec"]["template"]["spec"]
    assert pod["containers"][0] == {
        "name": "worker-7",
        "image": "registry/worker:1",
        "imagePullPolicy": "Always",
        "command": ["python", "-m", "worker"],
        "env": [{"name": "ORGANIZATION_ID", "value": "9"}],
        "envFrom": [{"configMapRef": {"name": "data-configmap"}}],
        "volumeMounts": [{"mountPath": "/etc/config/", "name": "data"}],
    }
    assert pod["imagePullSecrets"] == [{"name": "regcred"}]
    assert pod["volumes"] == [
        {
            "name": "data",
            "configMap": {"name": "worker-7-config", "items": [{"key": "config.json", "path": "config.json"}]},
        }
    ]


def test_deploy_update_patches_the_configmap_and_replaces_the_deployment(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = _stub_http(monkeypatch)
    WorkerHandler.deploy(_worker(), WorkerHandler.DeployOptions(update=True))

    assert [(method, path) for method, path, _ in sent] == [
        ("PATCH", f"{CONFIGMAPS}/worker-7-config"),
        ("PUT", f"{DEPLOYMENTS}/worker-7"),
    ]


@pytest.mark.parametrize("status", [200, 404])
def test_delete_removes_the_deployment_then_the_configmap(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    sent = _stub_http(monkeypatch, status)
    worker = _worker()
    worker.namespace = "workers"
    WorkerHandler.delete(worker)

    assert [(method, path) for method, path, _ in sent] == [
        ("DELETE", f"{DEPLOYMENTS}/worker-7"),
        ("DELETE", f"{CONFIGMAPS}/worker-7-config"),
    ]
    assert sent[0][2] == {"gracePeriodSeconds": 5, "propagationPolicy": "Foreground"}


def test_delete_retries_server_errors_then_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = _stub_http(monkeypatch, 500)
    worker = _worker()
    worker.namespace = "workers"
    with pytest.raises(ApiException) as exc_info:
        WorkerHandler.delete(worker)

    assert exc_info.value.status == 500
    assert len(sent) == WorkerHandler.DELETE_MAX_ATTEMPTS
