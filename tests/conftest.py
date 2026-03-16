from __future__ import annotations

# pylint: disable=import-outside-toplevel,unused-import,broad-exception-caught

import sys
import types
from typing import Any


def _ensure_app_settings() -> None:
    settings_module: Any = types.ModuleType("app.settings")
    settings_module.TS_AGGREGATION_INTERVAL_SECS = 60
    settings_module.KUBERNETES_SA_ENABLED = False
    settings_module.KUBERNETES_CONFIG = "dummy"

    app_module = types.ModuleType("app")
    sys.modules.setdefault("app", app_module)
    sys.modules["app.settings"] = settings_module


def _ensure_k8s_utils() -> None:
    try:
        import k8s_utils  # noqa: F401
    except Exception:
        k8s_utils_module: Any = types.ModuleType("k8s_utils")
        deployment_module: Any = types.ModuleType("k8s_utils.deployment")
        configmap_module: Any = types.ModuleType("k8s_utils.configmap")

        class DummyDeployment:
            def __init__(self, app_name: str, namespace: str, sa_enabled: bool, config_file: str) -> None:
                self.app_name = app_name
                self.namespace = namespace
                self.sa_enabled = sa_enabled
                self.config_file = config_file
                self.created = False
                self.updated = False
                self.deleted = False

            def add_env_from(self, _env_from: str) -> None:
                return None

            def create(self, **_kwargs: Any) -> None:
                self.created = True

            def update(self, **_kwargs: Any) -> None:
                self.updated = True

            def delete(self) -> None:
                self.deleted = True

        class DummyConfigMap:
            def __init__(self, name: str, sa_enabled: bool, namespace: str, config_file: str) -> None:
                self.name = name
                self.sa_enabled = sa_enabled
                self.namespace = namespace
                self.config_file = config_file
                self.created = False
                self.updated = False
                self.deleted = False

            def create(self, filename: str, content: str) -> None:
                _ = (filename, content)
                self.created = True

            def update(self, filename: str, content: str) -> None:
                _ = (filename, content)
                self.updated = True

            def delete(self) -> None:
                self.deleted = True

        deployment_module.Deployment = DummyDeployment
        configmap_module.ConfigMap = DummyConfigMap

        sys.modules["k8s_utils"] = k8s_utils_module
        sys.modules["k8s_utils.deployment"] = deployment_module
        sys.modules["k8s_utils.configmap"] = configmap_module


def _ensure_kafka() -> None:
    try:
        import kafka  # noqa: F401
    except Exception:
        kafka_module: Any = types.ModuleType("kafka")

        class DummyTopicPartition:
            def __init__(self, topic: str, partition: int) -> None:
                self.topic = topic
                self.partition = partition

        class DummyKafkaConsumer:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                _ = (args, kwargs)
                self.assigned: list[Any] = []
                self.seek_calls: list[Any] = []
                self._messages: list[Any] = []

            def assign(self, partitions: list[DummyTopicPartition]) -> None:
                self.assigned = partitions

            def seek(self, partition: DummyTopicPartition, offset: int) -> None:
                self.seek_calls.append((partition, offset))

            def __iter__(self) -> Any:
                return iter(self._messages)

        class DummyKafkaProducer:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                _ = (args, kwargs)
                self.sent: list[Any] = []

            def send(self, topic: str, key: bytes, value: Any) -> str:
                self.sent.append((topic, key, value))
                return "sent"

            def flush(self) -> str:
                return "flushed"

        kafka_module.KafkaConsumer = DummyKafkaConsumer
        kafka_module.KafkaProducer = DummyKafkaProducer
        kafka_module.TopicPartition = DummyTopicPartition

        sys.modules["kafka"] = kafka_module


def _ensure_paho() -> None:
    try:
        import paho.mqtt  # noqa: F401
    except Exception:
        paho_module: Any = types.ModuleType("paho")
        mqtt_module: Any = types.ModuleType("paho.mqtt")
        subscribe_module: Any = types.ModuleType("paho.mqtt.subscribe")
        publish_module: Any = types.ModuleType("paho.mqtt.publish")

        def _simple(*_args: Any, **_kwargs: Any) -> Any:
            return None

        def _single(*_args: Any, **_kwargs: Any) -> Any:
            return None

        subscribe_module.simple = _simple
        publish_module.single = _single
        mqtt_module.subscribe = subscribe_module
        mqtt_module.publish = publish_module

        sys.modules["paho"] = paho_module
        sys.modules["paho.mqtt"] = mqtt_module
        sys.modules["paho.mqtt.subscribe"] = subscribe_module
        sys.modules["paho.mqtt.publish"] = publish_module


def _ensure_redis() -> None:
    try:
        import redis  # noqa: F401
    except Exception:
        redis_module: Any = types.ModuleType("redis")
        redis_client_module: Any = types.ModuleType("redis.client")
        redis_cluster_module: Any = types.ModuleType("redis.cluster")

        class ResponseError(Exception):
            pass

        class DummyRedis:
            @classmethod
            def from_url(cls, _url: str) -> "DummyRedis":
                return cls()

        class DummyRedisCluster(DummyRedis):
            pass

        redis_module.ResponseError = ResponseError
        redis_client_module.Redis = DummyRedis
        redis_cluster_module.RedisCluster = DummyRedisCluster

        sys.modules["redis"] = redis_module
        sys.modules["redis.client"] = redis_client_module
        sys.modules["redis.cluster"] = redis_cluster_module


def _ensure_kubernetes() -> None:
    try:
        import kubernetes.client  # noqa: F401
    except Exception:
        kubernetes_module: Any = types.ModuleType("kubernetes")
        client_module: Any = types.ModuleType("kubernetes.client")

        class ApiException(Exception):
            def __init__(self, body: str | None = None, **kwargs: Any) -> None:
                _ = kwargs
                super().__init__(body or "")
                self.body = body or ""

        client_module.ApiException = ApiException
        kubernetes_module.client = client_module

        sys.modules["kubernetes"] = kubernetes_module
        sys.modules["kubernetes.client"] = client_module


def pytest_configure() -> None:
    _ensure_app_settings()
    _ensure_k8s_utils()
    _ensure_kafka()
    _ensure_paho()
    _ensure_redis()
    _ensure_kubernetes()
