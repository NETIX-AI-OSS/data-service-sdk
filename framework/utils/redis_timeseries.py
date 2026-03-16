from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Type, TypeVar, Union

from redis.client import Redis
from redis.cluster import RedisCluster

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="RedisTimeseries")


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
            self.redis_instance = RedisCluster.from_url(url)
        else:
            self.redis_instance = Redis.from_url(url)

    @classmethod
    def get_redis_instance(cls: Type[T]) -> Union[Redis, RedisCluster]:
        logger.info("Getting redis instance")
        if cls.cls_instance is None:
            cls.cls_instance = RedisTimeseries()
        if cls.cls_instance.redis_instance is None:
            raise RuntimeError("Redis instance not initialized")
        return cls.cls_instance.redis_instance
