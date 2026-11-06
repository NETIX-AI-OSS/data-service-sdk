from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from framework.handlers.subscription.factory import Factory

logger = logging.getLogger(__name__)


@dataclass
class SubscriptionManager:
    subscription_controller: Any | None = None

    def get_controller(self, config: dict[str, Any]) -> Any | None:
        controller = Factory.get_subscription_controller(config["type"], config)
        self.subscription_controller = controller
        return controller
