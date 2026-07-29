"""Idempotent quality table setup for the ALI Iceberg environment."""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


QUALITY_RESULTS_COLUMNS = """
    check_id STRING,
    check_time TIMESTAMP,
    source_table STRING,
    rule_name STRING,
    severity STRING,
    total_rows BIGINT,
    failed_rows BIGINT,
    status STRING,
    action_taken STRING,
    details STRING
"""

QUARANTINE_COLUMNS = """
    quarantine_id STRING,
    detected_at TIMESTAMP,
    source_table STRING,
    record_id STRING,
    failed_rule STRING,
    severity STRING,
    failure_reason STRING,
    raw_record STRING,
    raw_payload STRING,
    quarantine_status STRING
"""

TABLE_DDLS = [
    (
        "demo.quality.bronze_quality_results",
        f"""
        CREATE TABLE IF NOT EXISTS demo.quality.bronze_quality_results (
            {QUALITY_RESULTS_COLUMNS}
        )
        USING iceberg
        """,
    ),
    (
        "demo.quality.bronze_quarantine",
        f"""
        CREATE TABLE IF NOT EXISTS demo.quality.bronze_quarantine (
            {QUARANTINE_COLUMNS}
        )
        USING iceberg
        """,
    ),
    (
        "demo.quality.silver_quality_results",
        f"""
        CREATE TABLE IF NOT EXISTS demo.quality.silver_quality_results (
            {QUALITY_RESULTS_COLUMNS}
        )
        USING iceberg
        """,
    ),
    (
        "demo.quality.silver_quarantine",
        f"""
        CREATE TABLE IF NOT EXISTS demo.quality.silver_quarantine (
            {QUARANTINE_COLUMNS}
        )
        USING iceberg
        """,
    ),
]


def run_job() -> int:
    spark = (
        SparkSession.builder.appName("setup_quality_tables_job")
        .getOrCreate()
    )

    try:
        for table_name, ddl in TABLE_DDLS:
            logger.info("Creating or verifying quality table | table=%s", table_name)
            spark.sql(ddl)

        logger.info("Quality table setup completed | table_count=%s", len(TABLE_DDLS))
        return 0
    except Exception:
        logger.exception("Quality table setup failed.")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_job())
