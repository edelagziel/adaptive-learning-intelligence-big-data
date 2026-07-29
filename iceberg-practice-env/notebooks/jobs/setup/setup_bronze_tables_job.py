"""Idempotent Bronze table setup for the ALI Iceberg environment."""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


TABLE_DDLS = [
    (
        "demo.bronze.learning_events",
        """
        CREATE TABLE IF NOT EXISTS demo.bronze.learning_events (
            event_id STRING,
            user_id STRING,
            session_id STRING,
            event_type STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            source_system STRING,
            raw_payload STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.bronze.learning_feedback",
        """
        CREATE TABLE IF NOT EXISTS demo.bronze.learning_feedback (
            feedback_id STRING,
            user_id STRING,
            session_id STRING,
            practice_id STRING,
            feedback_stage STRING,
            feedback_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            raw_payload STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.bronze.reference_materials",
        """
        CREATE TABLE IF NOT EXISTS demo.bronze.reference_materials (
            reference_id STRING,
            batch_id STRING,
            source_type STRING,
            source_name STRING,
            file_name STRING,
            import_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            raw_payload STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.bronze.question_bank",
        """
        CREATE TABLE IF NOT EXISTS demo.bronze.question_bank (
            question_id STRING,
            question_version INT,
            source_system STRING,
            created_at TIMESTAMP,
            ingestion_time TIMESTAMP,
            raw_payload STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.bronze.learner_profiles",
        """
        CREATE TABLE IF NOT EXISTS demo.bronze.learner_profiles (
            user_id STRING,
            profile_updated_at TIMESTAMP,
            ingestion_time TIMESTAMP,
            source_system STRING,
            raw_payload STRING
        )
        USING iceberg
        """,
    ),
]


def run_job() -> int:
    spark = (
        SparkSession.builder.appName("setup_bronze_tables_job")
        .getOrCreate()
    )

    try:
        for table_name, ddl in TABLE_DDLS:
            logger.info("Creating or verifying Bronze table | table=%s", table_name)
            spark.sql(ddl)

        logger.info("Bronze table setup completed | table_count=%s", len(TABLE_DDLS))
        return 0
    except Exception:
        logger.exception("Bronze table setup failed.")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_job())
