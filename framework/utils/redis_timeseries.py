from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Type, TypeVar, Union

from redis.client import Redis
from redis.cluster import RedisCluster

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="RedisTimeseries")

# Keep the configured endpoint as the client's permanent startup node.
#
# redis-py defaults to ``dynamic_startup_nodes=True``, which *replaces*
# ``startup_nodes`` with the node addresses discovered via CLUSTER SLOTS --
# i.e. raw pod IPs -- as soon as the first topology load succeeds. The
# service DNS name the caller configured is then gone from the client for
# good. When every one of those pods is subsequently rescheduled onto new
# IPs (routine for a StatefulSet roll), the client is left holding nothing
# but dead addresses, has no way to re-resolve the service name, and can
# never recover: every command fails with "Redis Cluster cannot be
# connected. Please provide at least one reachable node" until the process
# is restarted. Because these clients are process-lifetime singletons, that
# means a permanently wedged worker.
#
# Pinning this to False keeps the (DNS) endpoint in ``startup_nodes``, so
# each re-initialisation re-resolves it and picks up the current topology.
_DYNAMIC_STARTUP_NODES = False

# Bound connection establishment so an address that no longer answers fails
# fast instead of tying up the caller for the OS-level TCP timeout. A stale
# pod IP black-holes the SYN, so without this every dead node costs seconds.
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
