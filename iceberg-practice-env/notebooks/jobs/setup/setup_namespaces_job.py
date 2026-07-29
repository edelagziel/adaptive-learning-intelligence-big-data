"""Idempotent namespace setup for the ALI Iceberg environment."""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


NAMESPACES = [
    "demo.bronze",
    "demo.silver",
    "demo.gold",
    "demo.quality",
]


def run_job() -> int:
    spark = (
        SparkSession.builder.appName("setup_namespaces_job")
        .getOrCreate()
    )

    try:
        for namespace in NAMESPACES:
            logger.info("Creating or verifying namespace | namespace=%s", namespace)
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        logger.info("Namespace setup completed | namespace_count=%s", len(NAMESPACES))
        return 0
    except Exception:
        logger.exception("Namespace setup failed.")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_job())
