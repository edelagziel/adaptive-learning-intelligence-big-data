"""Idempotent runtime path setup for ALI local jobs."""

from __future__ import annotations

import logging
import os
import sys


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


RUNTIME_PATHS = [
    "/home/iceberg/notebooks/notebooks/checkpoints/learning_events_kafka_to_bronze",
    "/home/iceberg/notebooks/notebooks/models",
]


def run_job() -> int:
    try:
        for path in RUNTIME_PATHS:
            logger.info("Creating or verifying runtime path | path=%s", path)
            os.makedirs(path, exist_ok=True)

        logger.info("Runtime path setup completed | path_count=%s", len(RUNTIME_PATHS))
        return 0
    except Exception:
        logger.exception("Runtime path setup failed.")
        return 1


if __name__ == "__main__":
    sys.exit(run_job())
