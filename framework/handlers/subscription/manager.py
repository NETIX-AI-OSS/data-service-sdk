from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from framework.handlers.subscription.factory import Factory

logger = logging.getLogger(__name__)


@dataclass
class SubscriptionManager:
    subscription_controller: Optional[Any] = None

    def get_controller(self, config: Dict[str, Any]) -> Optional[Any]:
        controller = Factory.get_subscription_controller(config["type"], config)
        self.subscription_controller = controller
        return controller
