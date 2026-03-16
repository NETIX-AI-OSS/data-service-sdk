from __future__ import annotations

import os
from abc import abstractmethod
from typing import Any, Dict, Optional


class BaseWorker:
    config: Dict[str, Any] = {}
    organization_id: Optional[str] = os.environ.get("ORGANIZATION_ID")

    def set_config(self, new_config: Dict[str, Any]) -> None:
        self.config = new_config

    @abstractmethod
    def loop(self) -> None:
        raise NotImplementedError
