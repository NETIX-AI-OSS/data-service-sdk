from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Type, TypeVar, Union

from redis.client import Redis
from redis.cluster import RedisCluster

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="RedisTimeseries")

# dynamic_startup_nodes=True loses DNS on pod IP churn, wedging worker forever
_DYNAMIC_STARTUP_NODES = False

# Bound connect/read timeouts so a stale pod IP fails fast, not OS TCP timeout
_SOCKET_CONNECT_TIMEOUT_SECS = float(os.environ.get("REDIS_SOCKET_CONNECT_TIMEOUT_SECS", "2"))
_SOCKET_TIMEOUT_SECS = float(os.environ.get("REDIS_SOCKET_TIMEOUT_SECS", "5"))


@dataclass
class RedisTimeseries:
    cls_instance: Optional["RedisTimeseries"] = None
    redis_instance: Optional[Union[Redis, RedisCluster]] = None

    def __init__(self) -> None:
        logger.info("redis producer init")
        redis_cluster = os.environ.get("REDIS_CLUSTER", "false")
        password = os.environ.get("REDIS_PASSWORD", "")
        host = os.environ.get("REDIS_HOST", "localhost")
        port = os.environ.get("REDIS_PORT", "6379")
        url = f"redis://:{password}@{host}:{port}"
        if redis_cluster == "true":
            self.redis_instance = RedisCluster.from_url(
                url,
                dynamic_startup_nodes=_DYNAMIC_STARTUP_NODES,
                socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT_SECS,
                socket_timeout=_SOCKET_TIMEOUT_SECS,
            )
        else:
            self.redis_instance = Redis.from_url(
                url,
                socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT_SECS,
                socket_timeout=_SOCKET_TIMEOUT_SECS,
            )

    @classmethod
    def get_redis_instance(cls: Type[T]) -> Union[Redis, RedisCluster]:
        logger.info("Getting redis instance")
        if cls.cls_instance is None:
            cls.cls_instance = RedisTimeseries()
        if cls.cls_instance.redis_instance is None:
            raise RuntimeError("Redis instance not initialized")
        return cls.cls_instance.redis_instance
