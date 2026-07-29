#!/usr/bin/env python
# coding: utf-8
"""Reusable PySpark job: load Bronze batch JSON sources via Iceberg MERGE INTO."""

from __future__ import annotations

import logging
import sys
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_json, to_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source paths
# ---------------------------------------------------------------------------
JSON_BASE_PATH = "/home/iceberg/notebooks/jasonData"

LEARNER_PROFILES_PATH = f"{JSON_BASE_PATH}/learner_profiles.json"
QUESTION_BANK_PATH = f"{JSON_BASE_PATH}/question_bank.json"
REFERENCE_MATERIALS_PATH = f"{JSON_BASE_PATH}/reference_materials.json"
LEARNING_FEEDBACK_PATH = f"{JSON_BASE_PATH}/learning_feedback.json"
LEARNING_EVENTS_PATH = f"{JSON_BASE_PATH}/learning_events_final.json"

# ---------------------------------------------------------------------------
# Target tables
# ---------------------------------------------------------------------------
LEARNER_PROFILES_TABLE = "demo.bronze.learner_profiles"
QUESTION_BANK_TABLE = "demo.bronze.question_bank"
REFERENCE_MATERIALS_TABLE = "demo.bronze.reference_materials"
LEARNING_FEEDBACK_TABLE = "demo.bronze.learning_feedback"
LEARNING_EVENTS_TABLE = "demo.bronze.learning_events"


def get_spark() -> SparkSession:
    """Return a SparkSession using container-provided Iceberg configuration."""
    return (
        SparkSession.builder
        .appName("bronze_batch_job")
        .getOrCreate()
    )


def transform_learner_profiles(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(LEARNER_PROFILES_PATH)
    )
    return raw_df.select(
        col("user_id"),
        to_timestamp(col("profile_updated_at")).alias("profile_updated_at"),
        to_timestamp(col("ingestion_time")).alias("ingestion_time"),
        col("source_system"),
        to_json(col("raw_payload")).alias("raw_payload"),
    )


def transform_question_bank(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(QUESTION_BANK_PATH)
    )
    return raw_df.select(
        col("question_id"),
        col("question_version").cast("int").alias("question_version"),
        col("source_system"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("ingestion_time")).alias("ingestion_time"),
        to_json(col("raw_payload")).alias("raw_payload"),
    )


def transform_reference_materials(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(REFERENCE_MATERIALS_PATH)
    )
    return raw_df.select(
        col("reference_id"),
        col("batch_id"),
        col("source_type"),
        col("source_name"),
        col("file_name"),
        to_timestamp(col("import_time")).alias("import_time"),
        to_timestamp(col("ingestion_time")).alias("ingestion_time"),
        to_json(col("raw_payload")).alias("raw_payload"),
    )


def transform_learning_feedback(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(LEARNING_FEEDBACK_PATH)
    )
    return raw_df.select(
        col("feedback_id"),
        col("user_id"),
        col("session_id"),
        col("practice_id"),
        col("feedback_stage"),
        to_timestamp(col("feedback_time")).alias("feedback_time"),
        to_timestamp(col("ingestion_time")).alias("ingestion_time"),
        to_json(col("raw_payload")).alias("raw_payload"),
    )


def transform_learning_events(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(LEARNING_EVENTS_PATH)
    )
    return raw_df.select(
        col("event_id"),
        col("user_id"),
        col("session_id"),
        col("event_type"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_timestamp(col("ingestion_time")).alias("ingestion_time"),
        col("source_system"),
        to_json(col("raw_payload")).alias("raw_payload"),
    )


def merge_bronze_table(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: List[str],
    insert_columns: List[str],
    temp_view_name: str,
    source_label: str,
) -> None:
    """Idempotent Iceberg MERGE INTO: insert-only when business keys do not match."""
    logger.info("Processing source=%s target=%s", source_label, target_table)

    source_count = source_df.count()
    logger.info("Source row count for %s: %s", source_label, source_count)

    valid_df = source_df.dropna(subset=merge_keys).dropDuplicates(merge_keys)
    valid_count = valid_df.count()
    logger.info(
        "Valid deduplicated row count for %s: %s",
        source_label,
        valid_count,
    )

    on_clause = " AND ".join(
        f"target.{key} = source.{key}" for key in merge_keys
    )
    insert_cols = ",\n                ".join(insert_columns)
    insert_vals = ",\n                ".join(
        f"source.{column}" for column in insert_columns
    )

    merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_view_name} AS source
            ON {on_clause}
            WHEN NOT MATCHED THEN INSERT (
                {insert_cols}
            )
            VALUES (
                {insert_vals}
            )
        """

    try:
        valid_df.createOrReplaceTempView(temp_view_name)
        spark.sql(merge_sql)
        logger.info("Completed MERGE into %s", target_table)
    finally:
        spark.catalog.dropTempView(temp_view_name)


def run_job(spark: SparkSession) -> None:
    merge_bronze_table(
        spark=spark,
        source_df=transform_learner_profiles(spark),
        target_table=LEARNER_PROFILES_TABLE,
        merge_keys=["user_id", "profile_updated_at"],
        insert_columns=[
            "user_id",
            "profile_updated_at",
            "ingestion_time",
            "source_system",
            "raw_payload",
        ],
        temp_view_name="bronze_batch_learner_profiles_src",
        source_label="learner_profiles",
    )

    merge_bronze_table(
        spark=spark,
        source_df=transform_question_bank(spark),
        target_table=QUESTION_BANK_TABLE,
        merge_keys=["question_id", "question_version"],
        insert_columns=[
            "question_id",
            "question_version",
            "source_system",
            "created_at",
            "ingestion_time",
            "raw_payload",
        ],
        temp_view_name="bronze_batch_question_bank_src",
        source_label="question_bank",
    )

    merge_bronze_table(
        spark=spark,
        source_df=transform_reference_materials(spark),
        target_table=REFERENCE_MATERIALS_TABLE,
        merge_keys=["reference_id"],
        insert_columns=[
            "reference_id",
            "batch_id",
            "source_type",
            "source_name",
            "file_name",
            "import_time",
            "ingestion_time",
            "raw_payload",
        ],
        temp_view_name="bronze_batch_reference_materials_src",
        source_label="reference_materials",
    )

    merge_bronze_table(
        spark=spark,
        source_df=transform_learning_feedback(spark),
        target_table=LEARNING_FEEDBACK_TABLE,
        merge_keys=["feedback_id"],
        insert_columns=[
            "feedback_id",
            "user_id",
            "session_id",
            "practice_id",
            "feedback_stage",
            "feedback_time",
            "ingestion_time",
            "raw_payload",
        ],
        temp_view_name="bronze_batch_learning_feedback_src",
        source_label="learning_feedback",
    )

    merge_bronze_table(
        spark=spark,
        source_df=transform_learning_events(spark),
        target_table=LEARNING_EVENTS_TABLE,
        merge_keys=["event_id"],
        insert_columns=[
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "event_time",
            "ingestion_time",
            "source_system",
            "raw_payload",
        ],
        temp_view_name="bronze_batch_learning_events_src",
        source_label="learning_events",
    )


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        run_job(spark)
        logger.info("Bronze batch job completed successfully.")
        return 0
    except Exception:
        logger.exception("Bronze batch job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
