from __future__ import annotations

import json
import logging
from typing import Any, Dict, cast

logger = logging.getLogger(__name__)


class ConfigHandler:
    @staticmethod
    def get_config(config: str) -> Dict[str, Any]:
        with open(f"/etc/config/{config}", encoding="utf-8") as config_file:
            loaded_config = cast(Dict[str, Any], json.load(config_file))
            logger.debug(loaded_config)
        return loaded_config

    @staticmethod
    def get_example_config(config: str) -> Dict[str, Any]:
        with open(f"/home/appuser/code/worker/utils/configs/examples/{config}", encoding="utf-8") as config_file:
            loaded_config = cast(Dict[str, Any], json.load(config_file))
            logger.debug(loaded_config)
        return loaded_config
