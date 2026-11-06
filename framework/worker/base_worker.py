from __future__ import annotations

import os
from abc import abstractmethod
from typing import Any


class BaseWorker:
    config: dict[str, Any] = {}  # noqa: RUF012  # replaced wholesale via set_config
    organization_id: str | None = os.environ.get("ORGANIZATION_ID")

    def set_config(self, new_config: dict[str, Any]) -> None:
        self.config = new_config

    @abstractmethod
    def loop(self) -> None:
        raise NotImplementedError
