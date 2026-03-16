from __future__ import annotations

import copy
import dataclasses
import json
import logging
from typing import Any, Callable, Dict, Protocol

# from kubernetes import client
from k8s_utils.deployment import Deployment
from k8s_utils.configmap import ConfigMap

try:
    from app.settings import KUBERNETES_SA_ENABLED, KUBERNETES_CONFIG
except ImportError:  # pragma: no cover
    KUBERNETES_SA_ENABLED = False
    KUBERNETES_CONFIG = ""
from kubernetes.client import ApiException

logger = logging.getLogger(__name__)


"""
worker type
         'tag_redis':
         'computation':
         'sample_thunderbolt':
         'sample_thunderbolt_redis':

"""


class WorkerHandler:
    DELETE_MAX_ATTEMPTS = 3

    class OutputDestination(Protocol):
        config: Dict[str, Any]

    class OutputDestinationManager(Protocol):
        def all(self) -> list["WorkerHandler.OutputDestination"]:
            pass

    class InputType(Protocol):
        input_id_internal: str

    class WorkerType(Protocol):
        image: str
        command: str
        namespace: str

    class Worker(Protocol):
        id: Any
        input_type: "WorkerHandler.InputType"
        output_destinations: "WorkerHandler.OutputDestinationManager"
        config: Dict[str, Any]
        full_config: Dict[str, Any]
        image: str
        command: str
        namespace: str
        type: "WorkerHandler.WorkerType"

    @dataclasses.dataclass
    class DeployOptions:
        worker_base_name: str = "worker"
        env_from: str = ""
        image_pull_secrets: str = ""
        update: bool = False
        organization_id: int = 0

    @staticmethod
    def build_full_config(worker: "WorkerHandler.Worker") -> Dict[str, Any]:
        input_controller_type = worker.input_type.input_id_internal
        output_handlers_config = []
        for output_config in worker.output_destinations.all():
            output_handlers_config.append(output_config.config)

        full_config = copy.deepcopy(worker.config)
        full_config["input_controller_type"] = input_controller_type
        if worker.config.get("output", False):
            full_config["output_config"]["output_handlers_config"] = output_handlers_config
        return full_config

    @staticmethod
    def get_config_map_name(worker: "WorkerHandler.Worker", worker_base_name: str = "worker") -> str:
        config_map_name = f"{worker_base_name}-{str(worker.id)}-config"
        return config_map_name

    @staticmethod
    def get_worker_name(worker: "WorkerHandler.Worker", worker_base_name: str = "worker") -> str:
        worker_name = f"{worker_base_name}-{str(worker.id)}"
        return worker_name

    @staticmethod
    def deploy(
        worker: "WorkerHandler.Worker",
        options: "WorkerHandler.DeployOptions | None" = None,
    ) -> None:
        opts = options or WorkerHandler.DeployOptions()
        if not worker.image:
            worker.image = worker.type.image
        if not worker.command:
            worker.command = worker.type.command
        if not worker.namespace:
            worker.namespace = worker.type.namespace

        config_map_name = WorkerHandler.deploy_config(worker, opts.worker_base_name, opts.update)
        worker_name = WorkerHandler.get_worker_name(worker, opts.worker_base_name)

        deploy = Deployment(
            app_name=worker_name,
            namespace=worker.namespace,
            sa_enabled=KUBERNETES_SA_ENABLED,
            config_file=KUBERNETES_CONFIG,
        )
        if opts.env_from:
            deploy.add_env_from(opts.env_from)

        logger.info(f"Deploying {worker_name}")  # pylint: disable=logging-fstring-interpolation
        if opts.update:
            deploy.update(
                image=worker.image,
                command=worker.command.split(),
                image_pull_secrets=opts.image_pull_secrets,
                image_pull_policy="Always",
                volume_name="data",
                volume_mount_path="/etc/config/",
                volume_configmap_name=config_map_name,
                volume_configmap_key="config.json",
                volume_configmap_path="config.json",
                env_vars={"ORGANIZATION_ID": str(opts.organization_id)},
            )
        else:
            deploy.create(
                image=worker.image,
                command=worker.command.split(),
                image_pull_secrets=opts.image_pull_secrets,
                image_pull_policy="Always",
                volume_name="data",
                volume_mount_path="/etc/config/",
                volume_configmap_name=config_map_name,
                volume_configmap_key="config.json",
                volume_configmap_path="config.json",
                env_vars={"ORGANIZATION_ID": str(opts.organization_id)},
            )

    @staticmethod
    def deploy_config(worker: "WorkerHandler.Worker", worker_base_name: str = "worker", update: bool = False) -> str:
        if not worker.full_config:
            worker.full_config = WorkerHandler.build_full_config(worker)

        config_map_name = WorkerHandler.get_config_map_name(worker, worker_base_name)
        logger.info(config_map_name)
        config = ConfigMap(
            name=config_map_name,
            sa_enabled=KUBERNETES_SA_ENABLED,
            namespace=worker.namespace,
            config_file=KUBERNETES_CONFIG,
        )
        if update:
            config.update(filename="config.json", content=json.dumps(worker.full_config))
        else:
            config.create(filename="config.json", content=json.dumps(worker.full_config))
        return config_map_name

    @staticmethod
    def _is_not_found_error(error: ApiException) -> bool:
        try:
            body = json.loads(str(error.body))
        except AttributeError, TypeError, json.JSONDecodeError:
            return False
        return isinstance(body, dict) and body.get("code") == 404

    @staticmethod
    def _delete_resource(delete_func: Callable[[], None], resource_name: str) -> None:
        for attempt in range(1, WorkerHandler.DELETE_MAX_ATTEMPTS + 1):  # pragma: no branch
            try:
                delete_func()
                return
            except ApiException as error:
                if WorkerHandler._is_not_found_error(error):
                    return
                if attempt == WorkerHandler.DELETE_MAX_ATTEMPTS:
                    logger.error("Error deleting %s: %s", resource_name, error)
                    raise
                logger.warning(
                    "Retrying delete for %s (%d/%d): %s",
                    resource_name,
                    attempt,
                    WorkerHandler.DELETE_MAX_ATTEMPTS,
                    error,
                )

    @staticmethod
    def delete_config(worker: "WorkerHandler.Worker", worker_base_name: str = "worker") -> None:
        config_map_name = WorkerHandler.get_config_map_name(worker, worker_base_name)
        config = ConfigMap(
            name=config_map_name,
            sa_enabled=KUBERNETES_SA_ENABLED,
            namespace=worker.namespace,
            config_file=KUBERNETES_CONFIG,
        )
        WorkerHandler._delete_resource(config.delete, config_map_name)

    @staticmethod
    def update(worker: "WorkerHandler.Worker") -> None:
        WorkerHandler.delete(worker)
        WorkerHandler.deploy(worker, WorkerHandler.DeployOptions(env_from="data-configmap"))

    @staticmethod
    def check(worker: "WorkerHandler.Worker") -> None:
        _ = worker

    @staticmethod
    def delete(worker: "WorkerHandler.Worker", worker_base_name: str = "worker") -> None:
        worker_name = WorkerHandler.get_worker_name(worker, worker_base_name)
        deploy = Deployment(
            app_name=worker_name,
            namespace=worker.namespace,
            sa_enabled=KUBERNETES_SA_ENABLED,
            config_file=KUBERNETES_CONFIG,
        )
        WorkerHandler._delete_resource(deploy.delete, worker_name)
        WorkerHandler.delete_config(worker, worker_base_name)
