"""Idempotent ML table setup for the ALI Iceberg environment."""

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
        "demo.gold.ml_learning_difficulty_training",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.ml_learning_difficulty_training (
            user_key INT,
            topic_key INT,
            session_id STRING,
            avg_score_last_7_days FLOAT,
            failure_rate_last_7_days FLOAT,
            hints_used_last_7_days INT,
            avg_attempt_duration FLOAT,
            confidence_before_avg FLOAT,
            confidence_after_avg FLOAT,
            still_confused_rate FLOAT,
            illusion_gap_score FLOAT,
            repeated_mistake_count INT,
            extraction_confidence_avg FLOAT,
            reliability_score_avg FLOAT,
            overall_motivation_avg FLOAT,
            overall_stress_avg FLOAT,
            topic_self_reported_understanding_avg FLOAT,
            topic_confidence_avg FLOAT,
            session_avg_score FLOAT,
            struggle_label DOUBLE,
            training_row_created_at TIMESTAMP
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.ml_learning_difficulty_predictions",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.ml_learning_difficulty_predictions (
            user_key INT,
            topic_key INT,
            session_id STRING,
            struggle_label DOUBLE,
            prediction DOUBLE,
            struggle_probability FLOAT,
            model_version STRING,
            prediction_created_at TIMESTAMP
        )
        USING iceberg
        """,
    ),
]


def run_job() -> int:
    spark = (
        SparkSession.builder.appName("setup_ml_tables_job")
        .getOrCreate()
    )

    try:
        for table_name, ddl in TABLE_DDLS:
            logger.info("Creating or verifying ML table | table=%s", table_name)
            spark.sql(ddl)

        logger.info("ML table setup completed | table_count=%s", len(TABLE_DDLS))
        return 0
    except Exception:
        logger.exception("ML table setup failed.")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_job())
