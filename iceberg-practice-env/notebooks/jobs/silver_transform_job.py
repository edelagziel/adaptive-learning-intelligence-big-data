#!/usr/bin/env python
# coding: utf-8
"""Reusable PySpark job: load Silver tables via Iceberg MERGE INTO.

This job ports the existing Silver notebook/export transformations into an
idempotent batch job. It does not create, alter, replace, empty, or remove
tables; it only merges rows into existing Silver tables.

Validation prototype note:
The original notebook ``10_validate_ai_insights.ipynb`` referenced
``best_reference_match_df`` without defining that intermediate DataFrame. To
complete the prototype, the project owner explicitly approved a deterministic
reconstruction: for each ``insight_id``, rank candidate references by
``semantic_match_score`` descending and ``reference_id`` ascending, then keep
``row_number() = 1``. This is not a recovered notebook cell. It selects exactly
one reference per insight, adds no new scoring factor, and does not use
``reliability_score`` to choose the winning reference.
"""

from __future__ import annotations

import logging
import sys
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bronze source tables
# ---------------------------------------------------------------------------
BRONZE_LEARNER_PROFILES_TABLE = "demo.bronze.learner_profiles"
BRONZE_QUESTION_BANK_TABLE = "demo.bronze.question_bank"
BRONZE_REFERENCE_MATERIALS_TABLE = "demo.bronze.reference_materials"
BRONZE_LEARNING_FEEDBACK_TABLE = "demo.bronze.learning_feedback"
BRONZE_LEARNING_EVENTS_TABLE = "demo.bronze.learning_events"

# ---------------------------------------------------------------------------
# Silver target tables
# ---------------------------------------------------------------------------
SILVER_LEARNER_PROFILES_TABLE = "demo.silver.learner_profiles"
SILVER_QUESTION_BANK_TABLE = "demo.silver.question_bank"
SILVER_REFERENCE_MATERIALS_TABLE = "demo.silver.reference_materials"
SILVER_LEARNING_EVENTS_TABLE = "demo.silver.learning_events"
SILVER_PRACTICE_ATTEMPTS_TABLE = "demo.silver.practice_attempts"
SILVER_PRE_PRACTICE_FEEDBACK_TABLE = "demo.silver.pre_practice_feedback"
SILVER_POST_PRACTICE_FEEDBACK_TABLE = "demo.silver.post_practice_feedback"
SILVER_LEARNER_CHECK_IN_TABLE = "demo.silver.learner_check_in"
SILVER_LEARNER_CHECK_IN_TOPICS_TABLE = "demo.silver.learner_check_in_topics"
SILVER_AI_EXTRACTED_INSIGHTS_TABLE = "demo.silver.ai_extracted_insights"
SILVER_VALIDATED_LEARNING_INSIGHTS_TABLE = (
    "demo.silver.validated_learning_insights"
)
SILVER_CONTENT_TAXONOMY_TABLE = "demo.silver.content_taxonomy"
SILVER_LEARNER_CONCEPT_EVIDENCE_TABLE = (
    "demo.silver.learner_concept_evidence"
)

# ---------------------------------------------------------------------------
# JSON schemas from the source notebooks
# ---------------------------------------------------------------------------
LEARNER_PROFILE_PAYLOAD_SCHEMA = """
declared_background_level STRING,
learning_goal STRING,
main_domain STRING,
preferred_language STRING,
registration_date STRING
"""

QUESTION_PAYLOAD_SCHEMA = """
content_hash STRING,
correct_option_letter STRING,
created_by STRING,
difficulty_level INT,
domain STRING,
generation_model STRING,
is_active BOOLEAN,
option_a_text STRING,
option_b_text STRING,
option_c_text STRING,
option_d_text STRING,
question_text STRING,
question_type STRING,
subtopic STRING,
topic STRING,
validation_status STRING
"""

REFERENCE_MATERIAL_PAYLOAD_SCHEMA = """
author_or_owner STRING,
content_text STRING,
domain STRING,
page_number INT,
reliability_level STRING,
section_name STRING,
subtopic STRING,
title STRING,
topic STRING
"""

LEARNING_EVENT_PAYLOAD_SCHEMA = """
answers ARRAY<STRUCT<
    attempt_duration_seconds: INT,
    hints_used: INT,
    question_id: STRING,
    question_version: INT,
    selected_option_letter: STRING
>>,
assistant_answer STRING,
completion_status STRING,
conversation_id STRING,
conversation_summary STRING,
detected_concepts ARRAY<STRING>,
difficulty_indicators ARRAY<STRING>,
important_points ARRAY<STRING>,
learner_intent STRING,
possible_confusion BOOLEAN,
practice_id STRING,
processing_model STRING,
started_at STRING,
submitted_at STRING,
topic_id STRING,
user_prompt STRING
"""

LEARNING_FEEDBACK_PAYLOAD_SCHEMA = """
confidence_after_score INT,
confidence_before_score INT,
expected_difficulty_score INT,
free_text STRING,
free_text_after STRING,
free_text_before STRING,
overall_confidence_score INT,
overall_motivation_score INT,
overall_stress_score INT,
perceived_difficulty_score INT,
perceived_understanding_after_score INT,
perceived_understanding_before_score INT,
still_confused BOOLEAN,
topics_feedback ARRAY<STRUCT<
    confidence_score: INT,
    perceived_understanding_score: INT,
    still_confused: BOOLEAN,
    topic_id: STRING
>>
"""


LEARNER_PROFILE_COLUMNS = [
    "user_id",
    "registration_date",
    "preferred_language",
    "background_level",
    "learning_goal",
    "main_domain",
    "profile_updated_at",
    "ingestion_time",
    "is_current",
]

QUESTION_BANK_COLUMNS = [
    "question_id",
    "question_version",
    "question_text",
    "question_type",
    "domain",
    "topic",
    "subtopic",
    "difficulty_level",
    "option_a_text",
    "option_b_text",
    "option_c_text",
    "option_d_text",
    "correct_option_letter",
    "created_by",
    "generation_model",
    "created_at",
    "is_active",
    "validation_status",
    "content_hash",
]

REFERENCE_MATERIAL_COLUMNS = [
    "reference_id",
    "batch_id",
    "source_type",
    "source_name",
    "file_name",
    "import_time",
    "ingestion_time",
    "domain",
    "title",
    "topic",
    "content_text",
    "reliability_level",
    "author_or_owner",
    "content_hash",
    "is_active",
]

LEARNING_EVENT_COLUMNS = [
    "event_id",
    "user_id",
    "session_id",
    "event_type",
    "event_time",
    "ingestion_time",
    "source_system",
    "event_date",
    "event_hour",
    "payload_valid",
    "processing_status",
]

PRACTICE_ATTEMPT_COLUMNS = [
    "attempt_id",
    "event_id",
    "user_id",
    "session_id",
    "practice_id",
    "question_id",
    "question_version",
    "attempt_time",
    "selected_option_letter",
    "is_correct",
    "score",
    "hints_used",
    "attempt_duration_seconds",
    "attempt_number",
]

PRE_PRACTICE_FEEDBACK_COLUMNS = [
    "feedback_id",
    "user_id",
    "session_id",
    "practice_id",
    "feedback_time",
    "ingestion_time",
    "delay_minutes",
    "confidence_before_score",
    "perceived_understanding_before_score",
    "expected_difficulty_score",
    "free_text_before",
]

POST_PRACTICE_FEEDBACK_COLUMNS = [
    "feedback_id",
    "user_id",
    "session_id",
    "practice_id",
    "feedback_time",
    "ingestion_time",
    "delay_minutes",
    "confidence_after_score",
    "perceived_understanding_after_score",
    "perceived_difficulty_score",
    "still_confused",
    "free_text_after",
]

LEARNER_CHECK_IN_COLUMNS = [
    "feedback_id",
    "user_id",
    "session_id",
    "feedback_time",
    "ingestion_time",
    "delay_minutes",
    "overall_confidence_score",
    "overall_motivation_score",
    "overall_stress_score",
    "free_text",
]

LEARNER_CHECK_IN_TOPIC_COLUMNS = [
    "feedback_id",
    "user_id",
    "session_id",
    "topic_id",
    "feedback_time",
    "perceived_understanding_score",
    "topic_confidence_score",
    "still_confused",
]

AI_EXTRACTED_INSIGHT_COLUMNS = [
    "insight_id",
    "event_id",
    "user_id",
    "session_id",
    "extracted_at",
    "dynamic_concept_name",
    "extraction_confidence",
    "processing_model",
    "validation_status",
    "ai_attributes",
]

VALIDATED_LEARNING_INSIGHT_COLUMNS = [
    "validation_id",
    "insight_id",
    "reference_id",
    "user_id",
    "session_id",
    "dynamic_concept_name",
    "validation_time",
    "semantic_match_score",
    "reliability_score",
    "contradiction_flag",
    "validation_notes",
    "validation_status",
]

CONTENT_TAXONOMY_COLUMNS = [
    "taxonomy_id",
    "domain",
    "topic",
    "subtopic",
    "concept_name",
    "normalized_domain",
    "normalized_topic",
    "normalized_subtopic",
    "normalized_concept_name",
    "taxonomy_level",
    "parent_taxonomy_id",
    "source_type",
    "first_detected_at",
    "last_detected_at",
    "validation_status",
    "is_active",
    "taxonomy_hash",
]

LEARNER_CONCEPT_EVIDENCE_COLUMNS = [
    "evidence_id",
    "user_id",
    "session_id",
    "taxonomy_id",
    "event_id",
    "attempt_id",
    "feedback_id",
    "insight_id",
    "validation_id",
    "evidence_type",
    "evidence_time",
    "is_correct",
    "score",
    "hints_used",
    "attempt_duration_seconds",
    "attempt_number",
    "extraction_confidence",
    "semantic_match_score",
    "reliability_score",
    "contradiction_flag",
    "confidence_score",
    "perceived_understanding_score",
    "perceived_difficulty_score",
    "still_confused",
    "source_table",
    "processing_time",
]


def get_spark() -> SparkSession:
    """Return a SparkSession using container-provided Iceberg configuration."""
    return SparkSession.builder.appName("silver_transform_job").getOrCreate()


def non_key_columns(columns: Sequence[str], merge_keys: Sequence[str]) -> List[str]:
    return [column for column in columns if column not in set(merge_keys)]


def merge_silver_table(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: Sequence[str],
    columns: Sequence[str],
    temp_view_name: str,
    stage_label: str,
) -> None:
    """Idempotent Iceberg MERGE INTO, updating only non-key columns."""
    logger.info("Stage started: %s", stage_label)
    source_count = source_df.count()
    logger.info("%s source/transformed row count: %s", stage_label, source_count)

    valid_condition = None
    for key in merge_keys:
        key_condition = F.col(key).isNotNull()
        valid_condition = (
            key_condition if valid_condition is None else valid_condition & key_condition
        )

    valid_df = source_df.filter(valid_condition).select(*columns)
    valid_count = valid_df.count()
    deduplicated_df = valid_df.dropDuplicates(list(merge_keys))
    deduplicated_count = deduplicated_df.count()
    logger.info(
        "%s valid row count: %s; deduplicated row count: %s",
        stage_label,
        valid_count,
        deduplicated_count,
    )

    update_columns = non_key_columns(columns, merge_keys)
    on_clause = " AND ".join(
        f"target.{key} = source.{key}" for key in merge_keys
    )
    update_clause = ",\n                ".join(
        f"target.{column} = source.{column}" for column in update_columns
    )
    insert_columns = ",\n                ".join(columns)
    insert_values = ",\n                ".join(
        f"source.{column}" for column in columns
    )

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view_name} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET
                {update_clause}
        WHEN NOT MATCHED THEN INSERT (
                {insert_columns}
        )
        VALUES (
                {insert_values}
        )
    """

    try:
        deduplicated_df.createOrReplaceTempView(temp_view_name)
        spark.sql(merge_sql)
        logger.info("Completed MERGE into %s", target_table)
    finally:
        spark.catalog.dropTempView(temp_view_name)


def normalize_column(column: F.Column) -> F.Column:
    return F.lower(F.trim(F.regexp_replace(column, r"\s+", " ")))


def normalize_text(column_name: str) -> F.Column:
    return normalize_column(F.col(column_name))


def parsed_learning_feedback(spark: SparkSession) -> DataFrame:
    return spark.table(BRONZE_LEARNING_FEEDBACK_TABLE).withColumn(
        "payload",
        F.from_json(F.col("raw_payload"), LEARNING_FEEDBACK_PAYLOAD_SCHEMA),
    )


def transform_learner_profiles(spark: SparkSession) -> DataFrame:
    bronze_df = spark.table(BRONZE_LEARNER_PROFILES_TABLE)
    logger.info(
        "%s row count: %s",
        BRONZE_LEARNER_PROFILES_TABLE,
        bronze_df.count(),
    )
    parsed_df = bronze_df.withColumn(
        "payload",
        F.from_json(F.col("raw_payload"), LEARNER_PROFILE_PAYLOAD_SCHEMA),
    )
    profile_window = Window.partitionBy("user_id").orderBy(
        F.col("profile_updated_at").desc()
    )
    return (
        parsed_df
        .select(
            F.col("user_id"),
            F.to_date(F.col("payload.registration_date")).alias("registration_date"),
            F.trim(F.col("payload.preferred_language")).alias("preferred_language"),
            F.initcap(F.trim(F.col("payload.declared_background_level"))).alias(
                "background_level"
            ),
            F.trim(F.col("payload.learning_goal")).alias("learning_goal"),
            F.trim(F.col("payload.main_domain")).alias("main_domain"),
            F.col("profile_updated_at"),
            F.col("ingestion_time"),
        )
        .withColumn("profile_rank", F.row_number().over(profile_window))
        .withColumn(
            "is_current",
            F.when(F.col("profile_rank") == 1, True).otherwise(False),
        )
        .select(*LEARNER_PROFILE_COLUMNS)
    )


def transform_question_bank(spark: SparkSession) -> DataFrame:
    bronze_df = spark.table(BRONZE_QUESTION_BANK_TABLE)
    logger.info("%s row count: %s", BRONZE_QUESTION_BANK_TABLE, bronze_df.count())
    parsed_df = bronze_df.withColumn(
        "payload",
        F.from_json(F.col("raw_payload"), QUESTION_PAYLOAD_SCHEMA),
    )
    return parsed_df.select(
        F.col("question_id"),
        F.col("question_version"),
        F.trim(F.col("payload.question_text")).alias("question_text"),
        F.lower(F.trim(F.col("payload.question_type"))).alias("question_type"),
        F.trim(F.col("payload.domain")).alias("domain"),
        F.trim(F.col("payload.topic")).alias("topic"),
        F.trim(F.col("payload.subtopic")).alias("subtopic"),
        F.col("payload.difficulty_level").alias("difficulty_level"),
        F.trim(F.col("payload.option_a_text")).alias("option_a_text"),
        F.trim(F.col("payload.option_b_text")).alias("option_b_text"),
        F.trim(F.col("payload.option_c_text")).alias("option_c_text"),
        F.trim(F.col("payload.option_d_text")).alias("option_d_text"),
        F.upper(F.trim(F.col("payload.correct_option_letter"))).alias(
            "correct_option_letter"
        ),
        F.trim(F.col("payload.created_by")).alias("created_by"),
        F.trim(F.col("payload.generation_model")).alias("generation_model"),
        F.col("created_at"),
        F.col("payload.is_active").alias("is_active"),
        F.lower(F.trim(F.col("payload.validation_status"))).alias(
            "validation_status"
        ),
        F.trim(F.col("payload.content_hash")).alias("content_hash"),
    )


def transform_reference_materials(spark: SparkSession) -> DataFrame:
    bronze_df = spark.table(BRONZE_REFERENCE_MATERIALS_TABLE)
    logger.info(
        "%s row count: %s",
        BRONZE_REFERENCE_MATERIALS_TABLE,
        bronze_df.count(),
    )
    parsed_df = bronze_df.withColumn(
        "payload",
        F.from_json(F.col("raw_payload"), REFERENCE_MATERIAL_PAYLOAD_SCHEMA),
    )
    selected_df = parsed_df.select(
        F.col("reference_id"),
        F.col("batch_id"),
        F.trim(F.col("source_type")).alias("source_type"),
        F.trim(F.col("source_name")).alias("source_name"),
        F.trim(F.col("file_name")).alias("file_name"),
        F.col("import_time"),
        F.col("ingestion_time"),
        F.trim(F.col("payload.domain")).alias("domain"),
        F.trim(F.col("payload.title")).alias("title"),
        F.trim(F.col("payload.topic")).alias("topic"),
        F.trim(F.col("payload.content_text")).alias("content_text"),
        F.lower(F.trim(F.col("payload.reliability_level"))).alias(
            "reliability_level"
        ),
        F.trim(F.col("payload.author_or_owner")).alias("author_or_owner"),
    )
    return (
        selected_df
        .withColumn(
            "content_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("title"),
                    F.col("content_text"),
                    F.col("author_or_owner"),
                ),
                256,
            ),
        )
        .withColumn("is_active", F.lit(True))
        .select(*REFERENCE_MATERIAL_COLUMNS)
    )


def transform_learning_events(spark: SparkSession) -> DataFrame:
    bronze_df = spark.table(BRONZE_LEARNING_EVENTS_TABLE)
    logger.info("%s row count: %s", BRONZE_LEARNING_EVENTS_TABLE, bronze_df.count())
    selected_df = (
        bronze_df
        .withColumn(
            "parsed_payload",
            F.from_json(F.col("raw_payload"), LEARNING_EVENT_PAYLOAD_SCHEMA),
        )
        .select(
            F.col("event_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.lower(F.trim(F.col("event_type"))).alias("event_type"),
            F.col("event_time"),
            F.col("ingestion_time"),
            F.lower(F.trim(F.col("source_system"))).alias("source_system"),
            F.to_date(F.col("event_time")).alias("event_date"),
            F.hour(F.col("event_time")).alias("event_hour"),
            F.col("parsed_payload"),
        )
    )
    return (
        selected_df
        .withColumn("payload_valid", F.col("parsed_payload").isNotNull())
        .withColumn(
            "processing_status",
            F.when(F.col("payload_valid"), F.lit("processed")).otherwise(
                F.lit("failed")
            ),
        )
        .select(*LEARNING_EVENT_COLUMNS)
    )


def transform_practice_attempts(spark: SparkSession) -> DataFrame:
    bronze_df = spark.table(BRONZE_LEARNING_EVENTS_TABLE)
    question_df = spark.table(SILVER_QUESTION_BANK_TABLE).select(
        "question_id",
        "question_version",
        "correct_option_letter",
    )
    logger.info("%s row count: %s", BRONZE_LEARNING_EVENTS_TABLE, bronze_df.count())
    logger.info("%s row count: %s", SILVER_QUESTION_BANK_TABLE, question_df.count())

    practice_events_df = (
        bronze_df
        .filter(F.col("event_type") == "practice_submitted")
        .withColumn(
            "parsed_payload",
            F.from_json(F.col("raw_payload"), LEARNING_EVENT_PAYLOAD_SCHEMA),
        )
        .withColumn("answer", F.explode(F.col("parsed_payload.answers")))
    )
    attempt_window = Window.partitionBy(
        F.col("user_id"),
        F.col("answer.question_id"),
    ).orderBy(F.col("event_time"), F.col("event_id"))

    return (
        practice_events_df.alias("event")
        .join(
            question_df.alias("question"),
            (
                F.col("event.answer.question_id")
                == F.col("question.question_id")
            )
            & (
                F.col("event.answer.question_version")
                == F.col("question.question_version")
            ),
            "left",
        )
        .withColumn("attempt_number", F.row_number().over(attempt_window))
        .withColumn(
            "is_correct",
            F.col("event.answer.selected_option_letter")
            == F.col("question.correct_option_letter"),
        )
        .withColumn(
            "score",
            F.when(F.col("is_correct"), F.lit(1.0))
            .otherwise(F.lit(0.0))
            .cast("float"),
        )
        .withColumn(
            "attempt_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("event.event_id"),
                    F.col("event.answer.question_id"),
                    F.col("attempt_number").cast("string"),
                ),
                256,
            ),
        )
        .select(
            F.col("attempt_id"),
            F.col("event.event_id").alias("event_id"),
            F.col("event.user_id").alias("user_id"),
            F.col("event.session_id").alias("session_id"),
            F.col("event.parsed_payload.practice_id").alias("practice_id"),
            F.col("event.answer.question_id").alias("question_id"),
            F.col("event.answer.question_version").alias("question_version"),
            F.col("event.event_time").alias("attempt_time"),
            F.col("event.answer.selected_option_letter").alias(
                "selected_option_letter"
            ),
            F.col("is_correct"),
            F.col("score"),
            F.col("event.answer.hints_used").alias("hints_used"),
            F.col("event.answer.attempt_duration_seconds").alias(
                "attempt_duration_seconds"
            ),
            F.col("attempt_number"),
        )
    )


def transform_pre_practice_feedback(spark: SparkSession) -> DataFrame:
    parsed_df = parsed_learning_feedback(spark)
    logger.info(
        "%s row count: %s",
        BRONZE_LEARNING_FEEDBACK_TABLE,
        parsed_df.count(),
    )
    return (
        parsed_df
        .filter(F.col("feedback_stage") == "before_practice")
        .select(
            F.col("feedback_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("practice_id"),
            F.col("feedback_time"),
            F.col("ingestion_time"),
            F.round(
                (
                    F.unix_timestamp(F.col("ingestion_time"))
                    - F.unix_timestamp(F.col("feedback_time"))
                )
                / 60
            ).cast("int").alias("delay_minutes"),
            F.col("payload.confidence_before_score").alias(
                "confidence_before_score"
            ),
            F.col("payload.perceived_understanding_before_score").alias(
                "perceived_understanding_before_score"
            ),
            F.col("payload.expected_difficulty_score").alias(
                "expected_difficulty_score"
            ),
            F.trim(F.col("payload.free_text_before")).alias("free_text_before"),
        )
    )


def transform_post_practice_feedback(spark: SparkSession) -> DataFrame:
    parsed_df = parsed_learning_feedback(spark)
    logger.info(
        "%s row count: %s",
        BRONZE_LEARNING_FEEDBACK_TABLE,
        parsed_df.count(),
    )
    return (
        parsed_df
        .filter(F.col("feedback_stage") == "after_practice")
        .select(
            F.col("feedback_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("practice_id"),
            F.col("feedback_time"),
            F.col("ingestion_time"),
            F.round(
                (
                    F.unix_timestamp(F.col("ingestion_time"))
                    - F.unix_timestamp(F.col("feedback_time"))
                )
                / 60
            ).cast("int").alias("delay_minutes"),
            F.col("payload.confidence_after_score").alias(
                "confidence_after_score"
            ),
            F.col("payload.perceived_understanding_after_score").alias(
                "perceived_understanding_after_score"
            ),
            F.col("payload.perceived_difficulty_score").alias(
                "perceived_difficulty_score"
            ),
            F.col("payload.still_confused").alias("still_confused"),
            F.trim(F.col("payload.free_text_after")).alias("free_text_after"),
        )
    )


def transform_learner_check_in(spark: SparkSession) -> DataFrame:
    parsed_df = parsed_learning_feedback(spark)
    logger.info(
        "%s row count: %s",
        BRONZE_LEARNING_FEEDBACK_TABLE,
        parsed_df.count(),
    )
    return (
        parsed_df
        .filter(F.col("feedback_stage") == "general_check_in")
        .select(
            F.col("feedback_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("feedback_time"),
            F.col("ingestion_time"),
            F.round(
                (
                    F.unix_timestamp(F.col("ingestion_time"))
                    - F.unix_timestamp(F.col("feedback_time"))
                )
                / 60
            ).cast("int").alias("delay_minutes"),
            F.col("payload.overall_confidence_score").alias(
                "overall_confidence_score"
            ),
            F.col("payload.overall_motivation_score").alias(
                "overall_motivation_score"
            ),
            F.col("payload.overall_stress_score").alias(
                "overall_stress_score"
            ),
            F.trim(F.col("payload.free_text")).alias("free_text"),
        )
    )


def transform_learner_check_in_topics(spark: SparkSession) -> DataFrame:
    parsed_df = parsed_learning_feedback(spark)
    logger.info(
        "%s row count: %s",
        BRONZE_LEARNING_FEEDBACK_TABLE,
        parsed_df.count(),
    )
    return (
        parsed_df
        .filter(F.col("feedback_stage") == "general_check_in")
        .withColumn("topic_feedback", F.explode(F.col("payload.topics_feedback")))
        .select(
            F.col("feedback_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("topic_feedback.topic_id").alias("topic_id"),
            F.col("feedback_time"),
            F.col("topic_feedback.perceived_understanding_score").alias(
                "perceived_understanding_score"
            ),
            F.col("topic_feedback.confidence_score").alias(
                "topic_confidence_score"
            ),
            F.col("topic_feedback.still_confused").alias("still_confused"),
        )
    )


def transform_ai_extracted_insights(spark: SparkSession) -> DataFrame:
    bronze_df = spark.table(BRONZE_LEARNING_EVENTS_TABLE)
    logger.info("%s row count: %s", BRONZE_LEARNING_EVENTS_TABLE, bronze_df.count())
    ai_interactions_df = (
        bronze_df
        .filter(F.col("event_type") == "ai_learning_interaction")
        .withColumn(
            "parsed_payload",
            F.from_json(F.col("raw_payload"), LEARNING_EVENT_PAYLOAD_SCHEMA),
        )
        .withColumn(
            "dynamic_concept_name",
            F.explode(F.col("parsed_payload.detected_concepts")),
        )
    )
    return (
        ai_interactions_df
        .withColumn(
            "insight_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("event_id"),
                    F.col("dynamic_concept_name"),
                ),
                256,
            ),
        )
        .select(
            F.col("insight_id"),
            F.col("event_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("ingestion_time").alias("extracted_at"),
            F.trim(F.col("dynamic_concept_name")).alias("dynamic_concept_name"),
            F.lit(1.0).cast("float").alias("extraction_confidence"),
            F.trim(F.col("parsed_payload.processing_model")).alias(
                "processing_model"
            ),
            F.lit("pending").alias("validation_status"),
            F.col("raw_payload").alias("ai_attributes"),
        )
    )


def transform_validated_learning_insights(spark: SparkSession) -> DataFrame:
    ai_insights_df = spark.table(SILVER_AI_EXTRACTED_INSIGHTS_TABLE)
    reference_materials_df = spark.table(SILVER_REFERENCE_MATERIALS_TABLE)
    logger.info(
        "%s row count: %s",
        SILVER_AI_EXTRACTED_INSIGHTS_TABLE,
        ai_insights_df.count(),
    )
    logger.info(
        "%s row count: %s",
        SILVER_REFERENCE_MATERIALS_TABLE,
        reference_materials_df.count(),
    )

    insights_prepared_df = (
        ai_insights_df
        .select(
            "insight_id",
            "event_id",
            "user_id",
            "session_id",
            "dynamic_concept_name",
            "extracted_at",
        )
        .withColumn("concept_normalized", F.lower(F.trim(F.col("dynamic_concept_name"))))
    )
    references_prepared_df = (
        reference_materials_df
        .filter(F.col("is_active") == True)
        .select(
            "reference_id",
            "domain",
            "title",
            "topic",
            "content_text",
            "reliability_level",
        )
        .withColumn(
            "reference_text",
            F.lower(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("domain"), F.lit("")),
                    F.coalesce(F.col("topic"), F.lit("")),
                    F.coalesce(F.col("title"), F.lit("")),
                    F.coalesce(F.col("content_text"), F.lit("")),
                )
            ),
        )
    )
    validation_candidates_df = insights_prepared_df.crossJoin(
        references_prepared_df
    )
    logger.info("Candidate comparisons: %s", validation_candidates_df.count())

    scored_candidates_df = (
        validation_candidates_df
        .withColumn(
            "title_match",
            F.when(
                F.expr("instr(lower(coalesce(title, '')), concept_normalized) > 0"),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "topic_match",
            F.when(
                F.expr("instr(lower(coalesce(topic, '')), concept_normalized) > 0"),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "content_match",
            F.when(
                F.expr(
                    """
                    instr(
                        lower(coalesce(content_text, '')),
                        concept_normalized
                    ) > 0
                    """
                ),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "semantic_match_score",
            (
                F.col("title_match") * 0.5
                + F.col("topic_match") * 0.3
                + F.col("content_match") * 0.2
            ).cast("float"),
        )
    )

    best_reference_window = Window.partitionBy("insight_id").orderBy(
        F.col("semantic_match_score").desc(),
        F.col("reference_id").asc(),
    )
    best_reference_match_df = (
        scored_candidates_df
        .withColumn("reference_rank", F.row_number().over(best_reference_window))
        .filter(F.col("reference_rank") == 1)
    )

    return (
        best_reference_match_df
        .withColumn(
            "semantic_score_rounded",
            F.round(F.col("semantic_match_score").cast("double"), 2),
        )
        .withColumn(
            "source_reliability_score",
            F.when(F.col("reliability_level") == "official", F.lit(1.0))
            .when(F.col("reliability_level") == "approved", F.lit(0.8))
            .when(F.col("reliability_level") == "external", F.lit(0.6))
            .otherwise(F.lit(0.4)),
        )
        .withColumn(
            "reliability_score_rounded",
            F.round(
                (
                    F.col("semantic_score_rounded") * 0.7
                    + F.col("source_reliability_score") * 0.3
                ),
                2,
            ),
        )
        .withColumn("contradiction_flag", F.lit(False))
        .withColumn(
            "validation_status",
            F.when(F.col("semantic_score_rounded") >= F.lit(0.7), F.lit("validated"))
            .when(F.col("semantic_score_rounded") >= F.lit(0.3), F.lit("weak_match"))
            .otherwise(F.lit("no_reference")),
        )
        .withColumn("validation_time", F.current_timestamp())
        .withColumn(
            "validation_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("insight_id"),
                    F.col("reference_id"),
                ),
                256,
            ),
        )
        .withColumn(
            "validation_notes",
            F.concat_ws(
                " | ",
                F.lit("Baseline rule-based validation"),
                F.concat_ws("", F.lit("Matched reference: "), F.col("title")),
                F.concat_ws(
                    "",
                    F.lit("Source reliability: "),
                    F.col("reliability_level"),
                ),
            ),
        )
        .withColumn(
            "semantic_match_score",
            F.col("semantic_score_rounded").cast("float"),
        )
        .withColumn(
            "reliability_score",
            F.col("reliability_score_rounded").cast("float"),
        )
        .select(*VALIDATED_LEARNING_INSIGHT_COLUMNS)
    )


def build_question_taxonomy(question_bank_df: DataFrame) -> DataFrame:
    prepared_questions_df = (
        question_bank_df
        .filter(
            F.col("domain").isNotNull()
            & F.col("topic").isNotNull()
            & F.col("subtopic").isNotNull()
        )
        .withColumn("normalized_domain", normalize_text("domain"))
        .withColumn("normalized_topic", normalize_text("topic"))
        .withColumn("normalized_subtopic", normalize_text("subtopic"))
    )

    domain_taxonomy_df = (
        prepared_questions_df
        .groupBy("domain", "normalized_domain")
        .agg(
            F.min("created_at").alias("first_detected_at"),
            F.max("created_at").alias("last_detected_at"),
            F.max(F.col("is_active").cast("int")).cast("boolean").alias("is_active"),
        )
        .withColumn("topic", F.lit(None).cast("string"))
        .withColumn("subtopic", F.lit(None).cast("string"))
        .withColumn("concept_name", F.lit(None).cast("string"))
        .withColumn("normalized_topic", F.lit(None).cast("string"))
        .withColumn("normalized_subtopic", F.lit(None).cast("string"))
        .withColumn("normalized_concept_name", F.lit(None).cast("string"))
        .withColumn("taxonomy_level", F.lit("domain"))
        .withColumn("parent_taxonomy_id", F.lit(None).cast("string"))
        .withColumn("source_type", F.lit("question_bank"))
        .withColumn("validation_status", F.lit("approved"))
        .withColumn(
            "taxonomy_hash",
            F.sha2(
                F.concat_ws("||", F.lit("domain"), F.col("normalized_domain")),
                256,
            ),
        )
        .withColumn("taxonomy_id", F.col("taxonomy_hash"))
    )

    topic_taxonomy_df = (
        prepared_questions_df
        .groupBy("domain", "topic", "normalized_domain", "normalized_topic")
        .agg(
            F.min("created_at").alias("first_detected_at"),
            F.max("created_at").alias("last_detected_at"),
            F.max(F.col("is_active").cast("int")).cast("boolean").alias("is_active"),
        )
        .withColumn("subtopic", F.lit(None).cast("string"))
        .withColumn("concept_name", F.lit(None).cast("string"))
        .withColumn("normalized_subtopic", F.lit(None).cast("string"))
        .withColumn("normalized_concept_name", F.lit(None).cast("string"))
        .withColumn("taxonomy_level", F.lit("topic"))
        .withColumn(
            "parent_taxonomy_id",
            F.sha2(
                F.concat_ws("||", F.lit("domain"), F.col("normalized_domain")),
                256,
            ),
        )
        .withColumn("source_type", F.lit("question_bank"))
        .withColumn("validation_status", F.lit("approved"))
        .withColumn(
            "taxonomy_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("topic"),
                    F.col("normalized_domain"),
                    F.col("normalized_topic"),
                ),
                256,
            ),
        )
        .withColumn("taxonomy_id", F.col("taxonomy_hash"))
    )

    subtopic_taxonomy_df = (
        prepared_questions_df
        .groupBy(
            "domain",
            "topic",
            "subtopic",
            "normalized_domain",
            "normalized_topic",
            "normalized_subtopic",
        )
        .agg(
            F.min("created_at").alias("first_detected_at"),
            F.max("created_at").alias("last_detected_at"),
            F.max(F.col("is_active").cast("int")).cast("boolean").alias("is_active"),
        )
        .withColumn("concept_name", F.lit(None).cast("string"))
        .withColumn("normalized_concept_name", F.lit(None).cast("string"))
        .withColumn("taxonomy_level", F.lit("subtopic"))
        .withColumn(
            "parent_taxonomy_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("topic"),
                    F.col("normalized_domain"),
                    F.col("normalized_topic"),
                ),
                256,
            ),
        )
        .withColumn("source_type", F.lit("question_bank"))
        .withColumn("validation_status", F.lit("approved"))
        .withColumn(
            "taxonomy_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("subtopic"),
                    F.col("normalized_domain"),
                    F.col("normalized_topic"),
                    F.col("normalized_subtopic"),
                ),
                256,
            ),
        )
        .withColumn("taxonomy_id", F.col("taxonomy_hash"))
    )

    return (
        domain_taxonomy_df.select(CONTENT_TAXONOMY_COLUMNS)
        .unionByName(topic_taxonomy_df.select(CONTENT_TAXONOMY_COLUMNS))
        .unionByName(subtopic_taxonomy_df.select(CONTENT_TAXONOMY_COLUMNS))
    )


def transform_content_taxonomy(spark: SparkSession) -> DataFrame:
    question_bank_df = spark.table(SILVER_QUESTION_BANK_TABLE)
    reference_materials_df = spark.table(SILVER_REFERENCE_MATERIALS_TABLE)
    ai_insights_df = spark.table(SILVER_AI_EXTRACTED_INSIGHTS_TABLE)
    validated_insights_df = spark.table(SILVER_VALIDATED_LEARNING_INSIGHTS_TABLE)
    logger.info("%s row count: %s", SILVER_QUESTION_BANK_TABLE, question_bank_df.count())
    logger.info(
        "%s row count: %s",
        SILVER_REFERENCE_MATERIALS_TABLE,
        reference_materials_df.count(),
    )
    logger.info(
        "%s row count: %s",
        SILVER_AI_EXTRACTED_INSIGHTS_TABLE,
        ai_insights_df.count(),
    )
    logger.info(
        "%s row count: %s",
        SILVER_VALIDATED_LEARNING_INSIGHTS_TABLE,
        validated_insights_df.count(),
    )

    question_taxonomy_df = build_question_taxonomy(question_bank_df)

    concept_candidates_df = (
        ai_insights_df.alias("ai")
        .join(
            validated_insights_df.alias("v"),
            F.col("ai.insight_id") == F.col("v.insight_id"),
            "left",
        )
        .join(
            reference_materials_df.alias("r"),
            F.col("v.reference_id") == F.col("r.reference_id"),
            "left",
        )
        .select(
            F.col("ai.insight_id"),
            F.col("ai.dynamic_concept_name"),
            F.col("ai.extracted_at"),
            F.col("ai.extraction_confidence"),
            F.col("v.validation_status"),
            F.col("v.semantic_match_score"),
            F.col("v.reliability_score"),
            F.col("r.domain"),
            F.col("r.topic"),
            F.col("r.title").alias("matched_reference_title"),
            F.col("r.reliability_level"),
        )
    )

    existing_topics_df = (
        question_taxonomy_df
        .filter(F.col("taxonomy_level") == "topic")
        .select(
            F.col("taxonomy_id").alias("existing_topic_taxonomy_id"),
            F.col("normalized_topic").alias("existing_topic_name"),
        )
    )
    existing_subtopics_df = (
        question_taxonomy_df
        .filter(F.col("taxonomy_level") == "subtopic")
        .select(
            F.col("taxonomy_id").alias("existing_subtopic_taxonomy_id"),
            F.col("normalized_subtopic").alias("existing_subtopic_name"),
        )
    )

    classified_concepts_df = (
        concept_candidates_df
        .withColumn(
            "normalized_dynamic_concept",
            normalize_column(F.col("dynamic_concept_name")),
        )
        .join(
            existing_topics_df,
            F.col("normalized_dynamic_concept") == F.col("existing_topic_name"),
            "left",
        )
        .join(
            existing_subtopics_df,
            F.col("normalized_dynamic_concept")
            == F.col("existing_subtopic_name"),
            "left",
        )
        .withColumn(
            "taxonomy_action",
            F.when(
                F.col("existing_topic_taxonomy_id").isNotNull(),
                F.lit("use_existing_topic"),
            )
            .when(
                F.col("existing_subtopic_taxonomy_id").isNotNull(),
                F.lit("use_existing_subtopic"),
            )
            .otherwise(F.lit("new_concept_candidate")),
        )
        .withColumn(
            "matched_existing_taxonomy_id",
            F.when(
                F.col("existing_topic_taxonomy_id").isNotNull(),
                F.col("existing_topic_taxonomy_id"),
            ).otherwise(F.col("existing_subtopic_taxonomy_id")),
        )
    )

    new_concept_candidates_df = classified_concepts_df.filter(
        F.col("taxonomy_action") == "new_concept_candidate"
    )

    existing_subtopic_parents_df = (
        question_taxonomy_df
        .filter(F.col("taxonomy_level") == "subtopic")
        .select(
            F.col("taxonomy_id").alias("candidate_subtopic_parent_id"),
            F.col("normalized_subtopic").alias("candidate_subtopic_name"),
            F.col("normalized_domain").alias("candidate_domain"),
            F.col("normalized_topic").alias("candidate_topic"),
        )
    )
    existing_topic_parents_df = (
        question_taxonomy_df
        .filter(F.col("taxonomy_level") == "topic")
        .select(
            F.col("taxonomy_id").alias("fallback_topic_parent_id"),
            F.col("normalized_domain").alias("topic_parent_domain"),
            F.col("normalized_topic").alias("topic_parent_name"),
        )
    )

    concepts_with_parent_candidates_df = (
        new_concept_candidates_df.alias("c")
        .join(
            existing_subtopic_parents_df.alias("s"),
            (
                F.col("c.normalized_dynamic_concept").isNotNull()
                & (
                    normalize_column(F.col("c.domain"))
                    == F.col("s.candidate_domain")
                )
                & (
                    normalize_column(F.col("c.topic"))
                    == F.col("s.candidate_topic")
                )
                & F.expr(
                    """
                    instr(
                        lower(coalesce(matched_reference_title, '')),
                        candidate_subtopic_name
                    ) > 0
                    """
                )
            ),
            "left",
        )
        .join(
            existing_topic_parents_df.alias("t"),
            (
                normalize_column(F.col("c.domain"))
                == F.col("t.topic_parent_domain")
            )
            & (
                normalize_column(F.col("c.topic"))
                == F.col("t.topic_parent_name")
            ),
            "left",
        )
        .withColumn(
            "selected_parent_taxonomy_id",
            F.when(
                F.col("candidate_subtopic_parent_id").isNotNull(),
                F.col("candidate_subtopic_parent_id"),
            ).otherwise(F.col("fallback_topic_parent_id")),
        )
        .withColumn(
            "semantic_score_rounded",
            F.round(F.col("semantic_match_score").cast("double"), 2),
        )
        .withColumn(
            "new_taxonomy_validation_status",
            F.when(
                (F.col("validation_status") == "validated")
                & (F.col("semantic_score_rounded") >= F.lit(0.7)),
                F.lit("approved"),
            ).otherwise(F.lit("pending")),
        )
    )

    concept_taxonomy_df = (
        concepts_with_parent_candidates_df
        .select(
            F.col("domain"),
            F.col("topic"),
            F.col("dynamic_concept_name").alias("concept_name"),
            F.col("normalized_dynamic_concept").alias("normalized_concept_name"),
            F.col("extracted_at"),
            F.col("new_taxonomy_validation_status").alias("validation_status"),
            F.col("selected_parent_taxonomy_id").alias("parent_taxonomy_id"),
        )
        .groupBy(
            "domain",
            "topic",
            "concept_name",
            "normalized_concept_name",
            "validation_status",
            "parent_taxonomy_id",
        )
        .agg(
            F.min("extracted_at").alias("first_detected_at"),
            F.max("extracted_at").alias("last_detected_at"),
        )
        .withColumn("normalized_domain", normalize_column(F.col("domain")))
        .withColumn("normalized_topic", normalize_column(F.col("topic")))
        .withColumn("subtopic", F.lit(None).cast("string"))
        .withColumn("normalized_subtopic", F.lit(None).cast("string"))
        .withColumn("taxonomy_level", F.lit("concept"))
        .withColumn("source_type", F.lit("ai_extraction"))
        .withColumn("is_active", F.lit(True))
        .withColumn(
            "taxonomy_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("concept"),
                    F.col("normalized_domain"),
                    F.col("normalized_topic"),
                    F.col("normalized_concept_name"),
                ),
                256,
            ),
        )
        .withColumn("taxonomy_id", F.col("taxonomy_hash"))
        .select(CONTENT_TAXONOMY_COLUMNS)
    )

    return question_taxonomy_df.unionByName(concept_taxonomy_df)


def taxonomy_matchable(taxonomy_df: DataFrame) -> DataFrame:
    return (
        taxonomy_df
        .filter(F.col("taxonomy_level").isin("topic", "subtopic", "concept"))
        .withColumn(
            "normalized_match_name",
            F.when(F.col("taxonomy_level") == "topic", F.col("normalized_topic"))
            .when(
                F.col("taxonomy_level") == "subtopic",
                F.col("normalized_subtopic"),
            )
            .otherwise(F.col("normalized_concept_name")),
        )
        .select(
            "taxonomy_id",
            "taxonomy_level",
            "validation_status",
            "normalized_match_name",
        )
    )


def evidence_columns(
    df: DataFrame,
    evidence_id_expr: F.Column,
    evidence_type: str,
    evidence_time_expr: F.Column,
    source_table: str,
) -> DataFrame:
    return (
        df
        .withColumn("evidence_id", evidence_id_expr)
        .withColumn("evidence_type", F.lit(evidence_type))
        .withColumn("evidence_time", evidence_time_expr)
        .withColumn("source_table", F.lit(source_table))
        .withColumn("processing_time", F.current_timestamp())
    )


def build_practice_evidence(
    practice_attempts_df: DataFrame,
    question_bank_df: DataFrame,
    taxonomy_df: DataFrame,
) -> DataFrame:
    practice_with_question_df = (
        practice_attempts_df.alias("p")
        .join(
            question_bank_df.alias("q"),
            (F.col("p.question_id") == F.col("q.question_id"))
            & (F.col("p.question_version") == F.col("q.question_version")),
            "left",
        )
        .select(
            F.col("p.attempt_id"),
            F.col("p.event_id"),
            F.col("p.user_id"),
            F.col("p.session_id"),
            F.col("p.question_id"),
            F.col("p.question_version"),
            F.col("p.attempt_time"),
            F.col("p.is_correct"),
            F.col("p.score"),
            F.col("p.hints_used"),
            F.col("p.attempt_duration_seconds"),
            F.col("p.attempt_number"),
            F.col("q.domain"),
            F.col("q.topic"),
            F.col("q.subtopic"),
            normalize_column(F.col("q.domain")).alias("normalized_domain"),
            normalize_column(F.col("q.topic")).alias("normalized_topic"),
            normalize_column(F.col("q.subtopic")).alias("normalized_subtopic"),
        )
    )

    practice_taxonomy_matches_df = (
        practice_with_question_df.alias("p")
        .join(
            taxonomy_df.filter(F.col("taxonomy_level") == "subtopic").alias("t"),
            (F.col("p.normalized_domain") == F.col("t.normalized_domain"))
            & (F.col("p.normalized_topic") == F.col("t.normalized_topic"))
            & (F.col("p.normalized_subtopic") == F.col("t.normalized_subtopic")),
            "left",
        )
        .select(
            F.col("p.attempt_id"),
            F.col("p.event_id"),
            F.col("p.user_id"),
            F.col("p.session_id"),
            F.col("p.question_id"),
            F.col("p.question_version"),
            F.col("p.attempt_time"),
            F.col("p.is_correct"),
            F.col("p.score"),
            F.col("p.hints_used"),
            F.col("p.attempt_duration_seconds"),
            F.col("p.attempt_number"),
            F.col("t.taxonomy_id"),
        )
    )

    return (
        evidence_columns(
            practice_taxonomy_matches_df,
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("practice_attempt"),
                    F.col("attempt_id"),
                    F.col("taxonomy_id"),
                ),
                256,
            ),
            "practice_attempt",
            F.col("attempt_time"),
            "silver_practice_attempts",
        )
        .withColumn("feedback_id", F.lit(None).cast("string"))
        .withColumn("insight_id", F.lit(None).cast("string"))
        .withColumn("validation_id", F.lit(None).cast("string"))
        .withColumn("extraction_confidence", F.lit(None).cast("float"))
        .withColumn("semantic_match_score", F.lit(None).cast("float"))
        .withColumn("reliability_score", F.lit(None).cast("float"))
        .withColumn("contradiction_flag", F.lit(None).cast("boolean"))
        .withColumn("confidence_score", F.lit(None).cast("int"))
        .withColumn("perceived_understanding_score", F.lit(None).cast("int"))
        .withColumn("perceived_difficulty_score", F.lit(None).cast("int"))
        .withColumn("still_confused", F.lit(None).cast("boolean"))
        .select(*LEARNER_CONCEPT_EVIDENCE_COLUMNS)
    )


def build_ai_insight_evidence(
    ai_insights_df: DataFrame,
    taxonomy_matchable_df: DataFrame,
) -> DataFrame:
    ai_taxonomy_matches_df = (
        ai_insights_df.alias("ai")
        .withColumn(
            "normalized_dynamic_concept",
            normalize_column(F.col("dynamic_concept_name")),
        )
        .join(
            taxonomy_matchable_df.alias("t"),
            F.col("normalized_dynamic_concept") == F.col("t.normalized_match_name"),
            "left",
        )
        .select(
            F.col("ai.insight_id"),
            F.col("ai.event_id"),
            F.col("ai.user_id"),
            F.col("ai.session_id"),
            F.col("ai.dynamic_concept_name"),
            F.col("ai.extracted_at"),
            F.col("ai.extraction_confidence"),
            F.col("ai.validation_status").alias("insight_validation_status"),
            F.col("t.taxonomy_id"),
            F.col("t.taxonomy_level"),
            F.col("t.validation_status").alias("taxonomy_validation_status"),
        )
    )

    return (
        evidence_columns(
            ai_taxonomy_matches_df,
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("ai_insight"),
                    F.col("insight_id"),
                    F.col("taxonomy_id"),
                ),
                256,
            ),
            "ai_insight",
            F.col("extracted_at"),
            "silver_ai_extracted_insights",
        )
        .withColumn("attempt_id", F.lit(None).cast("string"))
        .withColumn("feedback_id", F.lit(None).cast("string"))
        .withColumn("validation_id", F.lit(None).cast("string"))
        .withColumn("is_correct", F.lit(None).cast("boolean"))
        .withColumn("score", F.lit(None).cast("float"))
        .withColumn("hints_used", F.lit(None).cast("int"))
        .withColumn("attempt_duration_seconds", F.lit(None).cast("int"))
        .withColumn("attempt_number", F.lit(None).cast("int"))
        .withColumn("semantic_match_score", F.lit(None).cast("float"))
        .withColumn("reliability_score", F.lit(None).cast("float"))
        .withColumn("contradiction_flag", F.lit(None).cast("boolean"))
        .withColumn("confidence_score", F.lit(None).cast("int"))
        .withColumn("perceived_understanding_score", F.lit(None).cast("int"))
        .withColumn("perceived_difficulty_score", F.lit(None).cast("int"))
        .withColumn("still_confused", F.lit(None).cast("boolean"))
        .select(*LEARNER_CONCEPT_EVIDENCE_COLUMNS)
    )


def build_validated_insight_evidence(
    validated_insights_df: DataFrame,
    ai_insights_df: DataFrame,
    taxonomy_matchable_df: DataFrame,
) -> DataFrame:
    validated_with_ai_df = (
        validated_insights_df.alias("v")
        .join(
            ai_insights_df.alias("ai"),
            F.col("v.insight_id") == F.col("ai.insight_id"),
            "inner",
        )
        .join(
            taxonomy_matchable_df.alias("t"),
            normalize_column(F.col("v.dynamic_concept_name"))
            == F.col("t.normalized_match_name"),
            "left",
        )
        .select(
            F.col("v.validation_id"),
            F.col("v.insight_id"),
            F.col("ai.event_id"),
            F.col("v.user_id"),
            F.col("v.session_id"),
            F.col("v.dynamic_concept_name"),
            F.col("v.validation_time"),
            F.col("ai.extraction_confidence"),
            F.col("v.semantic_match_score"),
            F.col("v.reliability_score"),
            F.col("v.contradiction_flag"),
            F.col("v.validation_status"),
            F.col("t.taxonomy_id"),
            F.col("t.taxonomy_level"),
        )
    )

    return (
        evidence_columns(
            validated_with_ai_df,
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("validated_insight"),
                    F.col("validation_id"),
                    F.col("taxonomy_id"),
                ),
                256,
            ),
            "validated_insight",
            F.col("validation_time"),
            "silver_validated_learning_insights",
        )
        .withColumn("attempt_id", F.lit(None).cast("string"))
        .withColumn("feedback_id", F.lit(None).cast("string"))
        .withColumn("is_correct", F.lit(None).cast("boolean"))
        .withColumn("score", F.lit(None).cast("float"))
        .withColumn("hints_used", F.lit(None).cast("int"))
        .withColumn("attempt_duration_seconds", F.lit(None).cast("int"))
        .withColumn("attempt_number", F.lit(None).cast("int"))
        .withColumn("confidence_score", F.lit(None).cast("int"))
        .withColumn("perceived_understanding_score", F.lit(None).cast("int"))
        .withColumn("perceived_difficulty_score", F.lit(None).cast("int"))
        .withColumn("still_confused", F.lit(None).cast("boolean"))
        .select(*LEARNER_CONCEPT_EVIDENCE_COLUMNS)
    )


def build_practice_taxonomy(
    practice_attempts_df: DataFrame,
    question_bank_df: DataFrame,
    taxonomy_df: DataFrame,
) -> DataFrame:
    practice_topics_df = (
        practice_attempts_df.alias("p")
        .join(
            question_bank_df.alias("q"),
            (F.col("p.question_id") == F.col("q.question_id"))
            & (F.col("p.question_version") == F.col("q.question_version")),
            "inner",
        )
        .select(
            F.col("p.practice_id"),
            F.col("p.user_id"),
            F.col("p.session_id"),
            F.col("q.domain"),
            F.col("q.topic"),
            F.col("q.subtopic"),
        )
        .distinct()
    )

    return (
        practice_topics_df.alias("p")
        .join(
            taxonomy_df.filter(F.col("taxonomy_level") == "subtopic").alias("t"),
            (normalize_column(F.col("p.domain")) == F.col("t.normalized_domain"))
            & (normalize_column(F.col("p.topic")) == F.col("t.normalized_topic"))
            & (
                normalize_column(F.col("p.subtopic"))
                == F.col("t.normalized_subtopic")
            ),
            "left",
        )
        .select(
            F.col("p.practice_id"),
            F.col("p.user_id"),
            F.col("p.session_id"),
            F.col("t.taxonomy_id"),
        )
    )


def build_feedback_evidence(
    feedback_df: DataFrame,
    practice_taxonomy_df: DataFrame,
    evidence_type: str,
    source_table: str,
    confidence_expr: F.Column,
    understanding_expr: F.Column,
    difficulty_expr: F.Column,
    confused_expr: F.Column,
) -> DataFrame:
    joined_df = feedback_df.alias("f").join(
        practice_taxonomy_df.alias("p"),
        (F.col("f.practice_id") == F.col("p.practice_id"))
        & (F.col("f.user_id") == F.col("p.user_id")),
        "left",
    )
    return (
        evidence_columns(
            joined_df,
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(evidence_type),
                    F.col("f.feedback_id"),
                    F.col("p.taxonomy_id"),
                ),
                256,
            ),
            evidence_type,
            F.col("f.feedback_time"),
            source_table,
        )
        .withColumn("taxonomy_id", F.col("p.taxonomy_id"))
        .withColumn("event_id", F.lit(None).cast("string"))
        .withColumn("attempt_id", F.lit(None).cast("string"))
        .withColumn("insight_id", F.lit(None).cast("string"))
        .withColumn("validation_id", F.lit(None).cast("string"))
        .withColumn("is_correct", F.lit(None).cast("boolean"))
        .withColumn("score", F.lit(None).cast("float"))
        .withColumn("hints_used", F.lit(None).cast("int"))
        .withColumn("attempt_duration_seconds", F.lit(None).cast("int"))
        .withColumn("attempt_number", F.lit(None).cast("int"))
        .withColumn("extraction_confidence", F.lit(None).cast("float"))
        .withColumn("semantic_match_score", F.lit(None).cast("float"))
        .withColumn("reliability_score", F.lit(None).cast("float"))
        .withColumn("contradiction_flag", F.lit(None).cast("boolean"))
        .withColumn("confidence_score", confidence_expr)
        .withColumn("perceived_understanding_score", understanding_expr)
        .withColumn("perceived_difficulty_score", difficulty_expr)
        .withColumn("still_confused", confused_expr)
        .select(
            F.col("evidence_id"),
            F.col("f.user_id").alias("user_id"),
            F.col("f.session_id").alias("session_id"),
            F.col("taxonomy_id"),
            F.col("event_id"),
            F.col("attempt_id"),
            F.col("f.feedback_id").alias("feedback_id"),
            F.col("insight_id"),
            F.col("validation_id"),
            F.col("evidence_type"),
            F.col("evidence_time"),
            F.col("is_correct"),
            F.col("score"),
            F.col("hints_used"),
            F.col("attempt_duration_seconds"),
            F.col("attempt_number"),
            F.col("extraction_confidence"),
            F.col("semantic_match_score"),
            F.col("reliability_score"),
            F.col("contradiction_flag"),
            F.col("confidence_score"),
            F.col("perceived_understanding_score"),
            F.col("perceived_difficulty_score"),
            F.col("still_confused"),
            F.col("source_table"),
            F.col("processing_time"),
        )
    )


def build_check_in_evidence(
    check_in_topics_df: DataFrame,
    taxonomy_matchable_df: DataFrame,
) -> DataFrame:
    check_in_topics_prepared_df = check_in_topics_df.withColumn(
        "normalized_check_in_topic",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col("topic_id"), "^topic_", ""),
                    "_",
                    " ",
                )
            )
        ),
    )
    check_in_taxonomy_matches_df = (
        check_in_topics_prepared_df.alias("c")
        .join(
            taxonomy_matchable_df.alias("t"),
            F.col("c.normalized_check_in_topic")
            == F.col("t.normalized_match_name"),
            "left",
        )
        .select(
            F.col("c.feedback_id"),
            F.col("c.user_id"),
            F.col("c.session_id"),
            F.col("c.topic_id"),
            F.col("c.feedback_time"),
            F.col("c.perceived_understanding_score"),
            F.col("c.topic_confidence_score"),
            F.col("c.still_confused"),
            F.col("t.taxonomy_id"),
            F.col("t.taxonomy_level"),
            F.col("t.validation_status").alias("taxonomy_validation_status"),
        )
    )

    return (
        evidence_columns(
            check_in_taxonomy_matches_df,
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("check_in"),
                    F.col("feedback_id"),
                    F.col("taxonomy_id"),
                ),
                256,
            ),
            "check_in",
            F.col("feedback_time"),
            "silver_learner_check_in_topics",
        )
        .withColumn("event_id", F.lit(None).cast("string"))
        .withColumn("attempt_id", F.lit(None).cast("string"))
        .withColumn("insight_id", F.lit(None).cast("string"))
        .withColumn("validation_id", F.lit(None).cast("string"))
        .withColumn("is_correct", F.lit(None).cast("boolean"))
        .withColumn("score", F.lit(None).cast("float"))
        .withColumn("hints_used", F.lit(None).cast("int"))
        .withColumn("attempt_duration_seconds", F.lit(None).cast("int"))
        .withColumn("attempt_number", F.lit(None).cast("int"))
        .withColumn("extraction_confidence", F.lit(None).cast("float"))
        .withColumn("semantic_match_score", F.lit(None).cast("float"))
        .withColumn("reliability_score", F.lit(None).cast("float"))
        .withColumn("contradiction_flag", F.lit(None).cast("boolean"))
        .withColumn("confidence_score", F.col("topic_confidence_score"))
        .withColumn("perceived_difficulty_score", F.lit(None).cast("int"))
        .select(*LEARNER_CONCEPT_EVIDENCE_COLUMNS)
    )


def transform_learner_concept_evidence(spark: SparkSession) -> DataFrame:
    practice_attempts_df = spark.table(SILVER_PRACTICE_ATTEMPTS_TABLE)
    question_bank_df = spark.table(SILVER_QUESTION_BANK_TABLE)
    taxonomy_df = spark.table(SILVER_CONTENT_TAXONOMY_TABLE)
    ai_insights_df = spark.table(SILVER_AI_EXTRACTED_INSIGHTS_TABLE)
    validated_insights_df = spark.table(SILVER_VALIDATED_LEARNING_INSIGHTS_TABLE)
    pre_feedback_df = spark.table(SILVER_PRE_PRACTICE_FEEDBACK_TABLE)
    post_feedback_df = spark.table(SILVER_POST_PRACTICE_FEEDBACK_TABLE)
    check_in_topics_df = spark.table(SILVER_LEARNER_CHECK_IN_TOPICS_TABLE)

    for table_name, df in [
        (SILVER_PRACTICE_ATTEMPTS_TABLE, practice_attempts_df),
        (SILVER_QUESTION_BANK_TABLE, question_bank_df),
        (SILVER_CONTENT_TAXONOMY_TABLE, taxonomy_df),
        (SILVER_AI_EXTRACTED_INSIGHTS_TABLE, ai_insights_df),
        (SILVER_VALIDATED_LEARNING_INSIGHTS_TABLE, validated_insights_df),
        (SILVER_PRE_PRACTICE_FEEDBACK_TABLE, pre_feedback_df),
        (SILVER_POST_PRACTICE_FEEDBACK_TABLE, post_feedback_df),
        (SILVER_LEARNER_CHECK_IN_TOPICS_TABLE, check_in_topics_df),
    ]:
        logger.info("%s row count: %s", table_name, df.count())

    taxonomy_matchable_df = taxonomy_matchable(taxonomy_df)
    practice_taxonomy_df = build_practice_taxonomy(
        practice_attempts_df,
        question_bank_df,
        taxonomy_df,
    )

    practice_evidence_df = build_practice_evidence(
        practice_attempts_df,
        question_bank_df,
        taxonomy_df,
    )
    ai_insight_evidence_df = build_ai_insight_evidence(
        ai_insights_df,
        taxonomy_matchable_df,
    )
    validated_insight_evidence_df = build_validated_insight_evidence(
        validated_insights_df,
        ai_insights_df,
        taxonomy_matchable_df,
    )
    pre_feedback_evidence_df = build_feedback_evidence(
        feedback_df=pre_feedback_df,
        practice_taxonomy_df=practice_taxonomy_df,
        evidence_type="pre_feedback",
        source_table="silver_pre_practice_feedback",
        confidence_expr=F.col("f.confidence_before_score"),
        understanding_expr=F.col("f.perceived_understanding_before_score"),
        difficulty_expr=F.col("f.expected_difficulty_score"),
        confused_expr=F.lit(None).cast("boolean"),
    )
    post_feedback_evidence_df = build_feedback_evidence(
        feedback_df=post_feedback_df,
        practice_taxonomy_df=practice_taxonomy_df,
        evidence_type="post_feedback",
        source_table="silver_post_practice_feedback",
        confidence_expr=F.col("f.confidence_after_score"),
        understanding_expr=F.col("f.perceived_understanding_after_score"),
        difficulty_expr=F.col("f.perceived_difficulty_score"),
        confused_expr=F.col("f.still_confused"),
    )
    check_in_evidence_df = build_check_in_evidence(
        check_in_topics_df,
        taxonomy_matchable_df,
    )

    return (
        practice_evidence_df
        .unionByName(ai_insight_evidence_df)
        .unionByName(validated_insight_evidence_df)
        .unionByName(pre_feedback_evidence_df)
        .unionByName(post_feedback_evidence_df)
        .unionByName(check_in_evidence_df)
    )


def merge_stage(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: Sequence[str],
    columns: Sequence[str],
    temp_view_name: str,
    stage_label: str,
) -> None:
    merge_silver_table(
        spark=spark,
        source_df=source_df,
        target_table=target_table,
        merge_keys=merge_keys,
        columns=columns,
        temp_view_name=temp_view_name,
        stage_label=stage_label,
    )


def prepare_learner_concept_evidence_for_merge(source_df: DataFrame) -> DataFrame:
    transformed_count = source_df.count()
    rejected_taxonomy_count = source_df.filter(F.col("taxonomy_id").isNull()).count()
    valid_df = source_df.filter(F.col("taxonomy_id").isNotNull())
    valid_count = valid_df.count()

    logger.info(
        "learner_concept_evidence transformed evidence row count: %s",
        transformed_count,
    )
    logger.info(
        "learner_concept_evidence rows rejected because taxonomy_id is null: %s",
        rejected_taxonomy_count,
    )
    logger.info(
        "learner_concept_evidence valid evidence row count: %s",
        valid_count,
    )

    return valid_df


def run_job(spark: SparkSession) -> None:
    logger.info("Silver transform job started.")

    merge_stage(
        spark,
        transform_learner_profiles(spark),
        SILVER_LEARNER_PROFILES_TABLE,
        ["user_id", "profile_updated_at"],
        LEARNER_PROFILE_COLUMNS,
        "silver_transform_learner_profiles_src",
        "learner_profiles",
    )
    merge_stage(
        spark,
        transform_question_bank(spark),
        SILVER_QUESTION_BANK_TABLE,
        ["question_id", "question_version"],
        QUESTION_BANK_COLUMNS,
        "silver_transform_question_bank_src",
        "question_bank",
    )
    merge_stage(
        spark,
        transform_reference_materials(spark),
        SILVER_REFERENCE_MATERIALS_TABLE,
        ["reference_id"],
        REFERENCE_MATERIAL_COLUMNS,
        "silver_transform_reference_materials_src",
        "reference_materials",
    )
    merge_stage(
        spark,
        transform_learning_events(spark),
        SILVER_LEARNING_EVENTS_TABLE,
        ["event_id"],
        LEARNING_EVENT_COLUMNS,
        "silver_transform_learning_events_src",
        "learning_events",
    )
    merge_stage(
        spark,
        transform_practice_attempts(spark),
        SILVER_PRACTICE_ATTEMPTS_TABLE,
        ["attempt_id"],
        PRACTICE_ATTEMPT_COLUMNS,
        "silver_transform_practice_attempts_src",
        "practice_attempts",
    )
    merge_stage(
        spark,
        transform_pre_practice_feedback(spark),
        SILVER_PRE_PRACTICE_FEEDBACK_TABLE,
        ["feedback_id"],
        PRE_PRACTICE_FEEDBACK_COLUMNS,
        "silver_transform_pre_practice_feedback_src",
        "pre_practice_feedback",
    )
    merge_stage(
        spark,
        transform_post_practice_feedback(spark),
        SILVER_POST_PRACTICE_FEEDBACK_TABLE,
        ["feedback_id"],
        POST_PRACTICE_FEEDBACK_COLUMNS,
        "silver_transform_post_practice_feedback_src",
        "post_practice_feedback",
    )
    merge_stage(
        spark,
        transform_learner_check_in(spark),
        SILVER_LEARNER_CHECK_IN_TABLE,
        ["feedback_id"],
        LEARNER_CHECK_IN_COLUMNS,
        "silver_transform_learner_check_in_src",
        "learner_check_in",
    )
    merge_stage(
        spark,
        transform_learner_check_in_topics(spark),
        SILVER_LEARNER_CHECK_IN_TOPICS_TABLE,
        ["feedback_id", "topic_id"],
        LEARNER_CHECK_IN_TOPIC_COLUMNS,
        "silver_transform_learner_check_in_topics_src",
        "learner_check_in_topics",
    )
    merge_stage(
        spark,
        transform_ai_extracted_insights(spark),
        SILVER_AI_EXTRACTED_INSIGHTS_TABLE,
        ["insight_id"],
        AI_EXTRACTED_INSIGHT_COLUMNS,
        "silver_transform_ai_extracted_insights_src",
        "ai_extracted_insights",
    )
    merge_stage(
        spark,
        transform_validated_learning_insights(spark),
        SILVER_VALIDATED_LEARNING_INSIGHTS_TABLE,
        ["insight_id"],
        VALIDATED_LEARNING_INSIGHT_COLUMNS,
        "silver_transform_validated_learning_insights_src",
        "validated_learning_insights",
    )
    merge_stage(
        spark,
        transform_content_taxonomy(spark),
        SILVER_CONTENT_TAXONOMY_TABLE,
        ["taxonomy_id"],
        CONTENT_TAXONOMY_COLUMNS,
        "silver_transform_content_taxonomy_src",
        "content_taxonomy",
    )
    learner_concept_evidence_df = prepare_learner_concept_evidence_for_merge(
        transform_learner_concept_evidence(spark)
    )
    merge_stage(
        spark,
        learner_concept_evidence_df,
        SILVER_LEARNER_CONCEPT_EVIDENCE_TABLE,
        ["evidence_id"],
        LEARNER_CONCEPT_EVIDENCE_COLUMNS,
        "silver_transform_learner_concept_evidence_src",
        "learner_concept_evidence",
    )

    logger.info("Silver transform job completed successfully.")


def main() -> int:
    spark: Optional[SparkSession] = None
    try:
        spark = get_spark()
        run_job(spark)
        return 0
    except Exception:
        logger.exception("Silver transform job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
