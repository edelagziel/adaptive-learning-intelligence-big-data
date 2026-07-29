#!/usr/bin/env python
# coding: utf-8
"""
Bronze Data Quality MVP job.

This job:
- reads existing Bronze tables
- evaluates predefined quality rules
- writes rule results to demo.quality.bronze_quality_results
- writes audit copies of failed records to demo.quality.bronze_quarantine

This job does not:
- create, alter, rename, drop, truncate, or overwrite any table
- delete, move, repair, or update Bronze records
- change existing table schemas

Quarantine is an audit copy only.

Quarantine identity:
- quarantine_id is derived from source_table, record_id, failed_rule, and a
  SHA-256 hash of the full failed row (raw_record) so distinct physical rows
  are not collapsed when business keys collide or are null.
- The same failed row and failed rule are not duplicated across reruns.
- Existing quarantine records are never updated.

Kafka producer alignment:
- The learning-events producer now emits practice_submitted.
- Historical Bronze rows with event_type=practice_attempt are not changed
  automatically and may intentionally fail invalid_event_type until cleaned or
  retained as a quarantine demonstration.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from typing import Callable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Catalog and table constants
# ---------------------------------------------------------------------------
LEARNER_PROFILES_TABLE = "demo.bronze.learner_profiles"
QUESTION_BANK_TABLE = "demo.bronze.question_bank"
REFERENCE_MATERIALS_TABLE = "demo.bronze.reference_materials"
LEARNING_FEEDBACK_TABLE = "demo.bronze.learning_feedback"
LEARNING_EVENTS_TABLE = "demo.bronze.learning_events"

QUALITY_RESULTS_TABLE = "demo.quality.bronze_quality_results"
QUARANTINE_TABLE = "demo.quality.bronze_quarantine"

STATUS_PASS = "PASS"
STATUS_WARNING = "WARNING"
STATUS_FAIL = "FAIL"

SEVERITY_FAIL = "FAIL"
SEVERITY_WARNING = "WARNING"

OPEN_STATUS = "OPEN"
NULL_PLACEHOLDER = "__NULL__"

# ---------------------------------------------------------------------------
# JSON schemas from notebook checks (source of truth)
# ---------------------------------------------------------------------------
LEARNER_PROFILES_PAYLOAD_SCHEMA = """
declared_background_level STRING,
learning_goal STRING,
main_domain STRING,
preferred_language STRING,
registration_date STRING
"""

QUESTION_BANK_PAYLOAD_SCHEMA = """
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

REFERENCE_MATERIALS_PAYLOAD_SCHEMA = """
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

LEARNING_EVENTS_PAYLOAD_SCHEMA = """
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


@dataclass
class RuleOutput:
    source_table: str
    rule_name: str
    severity: str
    total_rows: int
    failed_rows: int
    status: str
    action_taken: str
    details: str


def get_spark() -> SparkSession:
    """Return SparkSession using container-provided Spark/Iceberg config."""
    return SparkSession.builder.appName("bronze_quality_job").getOrCreate()


def get_execution_identity() -> str:
    return (
        os.environ.get("AIRFLOW_CTX_EXECUTION_DATE")
        or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        or "manual"
    )


def deterministic_check_id(
    execution_identity: str,
    source_table: str,
    rule_name: str,
) -> str:
    raw = f"{execution_identity}|{source_table}|{rule_name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def status_for(failed_rows: int, severity: str) -> str:
    if failed_rows == 0:
        return STATUS_PASS
    if severity == SEVERITY_WARNING:
        return STATUS_WARNING
    return STATUS_FAIL


def build_record_id_expr(columns: List[str]) -> F.Column:
    return F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit(NULL_PLACEHOLDER))
            for column in columns
        ],
    )


def build_quarantine_candidates(
    failed_df: DataFrame,
    source_table: str,
    rule_name: str,
    severity: str,
    failure_reason: str,
    record_id_expr: F.Column,
    source_columns: List[str],
) -> DataFrame:
    with_record_id = failed_df.withColumn("record_id", record_id_expr)
    return (
        with_record_id
        .withColumn("source_table", F.lit(source_table))
        .withColumn("failed_rule", F.lit(rule_name))
        .withColumn("severity", F.lit(severity))
        .withColumn("failure_reason", F.lit(failure_reason))
        .withColumn("quarantine_status", F.lit(OPEN_STATUS))
        .withColumn(
            "raw_record",
            F.to_json(F.struct(*[F.col(column) for column in source_columns])),
        )
        .withColumn("raw_record_hash", F.sha2(F.col("raw_record"), 256))
        .withColumn(
            "quarantine_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("source_table"),
                    F.col("record_id"),
                    F.col("failed_rule"),
                    F.col("raw_record_hash"),
                ),
                256,
            ),
        )
        .withColumn("detected_at", F.current_timestamp())
        .select(
            "quarantine_id",
            "detected_at",
            "source_table",
            "record_id",
            "failed_rule",
            "severity",
            "failure_reason",
            "raw_record",
            "raw_payload",
            "quarantine_status",
        )
    )


def evaluate_rule(
    source_df: DataFrame,
    source_table: str,
    total_rows: int,
    rule_name: str,
    severity: str,
    details: str,
    failing_df_builder: Callable[[], DataFrame],
    record_id_expr: F.Column,
    supports_quarantine: bool = True,
) -> Tuple[RuleOutput, Optional[DataFrame], bool]:
    failed_df = failing_df_builder()
    failed_rows = failed_df.count()
    status = status_for(failed_rows, severity)
    has_critical_failure = severity == SEVERITY_FAIL and failed_rows > 0

    if failed_rows == 0:
        action_taken = "no_action"
    elif supports_quarantine:
        action_taken = "quarantine_audit_copy"
    else:
        action_taken = "result_only"

    quarantine_df = None
    if supports_quarantine and failed_rows > 0:
        quarantine_df = build_quarantine_candidates(
            failed_df=failed_df,
            source_table=source_table,
            rule_name=rule_name,
            severity=severity,
            failure_reason=details,
            record_id_expr=record_id_expr,
            source_columns=source_df.columns,
        )

    logger.info(
        "Rule evaluated | table=%s rule=%s severity=%s total_rows=%s failed_rows=%s status=%s",
        source_table,
        rule_name,
        severity,
        total_rows,
        failed_rows,
        status,
    )
    if supports_quarantine and failed_rows > 0:
        logger.info(
            "Rule quarantine candidate rows | table=%s rule=%s quarantine_candidate_rows=%s",
            source_table,
            rule_name,
            failed_rows,
        )

    return (
        RuleOutput(
            source_table=source_table,
            rule_name=rule_name,
            severity=severity,
            total_rows=total_rows,
            failed_rows=failed_rows,
            status=status,
            action_taken=action_taken,
            details=details,
        ),
        quarantine_df,
        has_critical_failure,
    )


def merge_quality_results(
    spark: SparkSession,
    execution_identity: str,
    check_time: datetime,
    results: List[RuleOutput],
) -> None:
    if not results:
        return

    rows = [
        (
            deterministic_check_id(execution_identity, result.source_table, result.rule_name),
            check_time,
            result.source_table,
            result.rule_name,
            result.severity,
            int(result.total_rows),
            int(result.failed_rows),
            result.status,
            result.action_taken,
            result.details,
        )
        for result in results
    ]

    source_df = spark.createDataFrame(
        rows,
        schema="""
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
        """,
    )
    view_name = "bronze_quality_results_src"
    source_df.createOrReplaceTempView(view_name)
    merge_sql = f"""
        MERGE INTO {QUALITY_RESULTS_TABLE} AS target
        USING {view_name} AS source
        ON target.check_id = source.check_id
        WHEN MATCHED THEN UPDATE SET
            target.check_time = source.check_time,
            target.source_table = source.source_table,
            target.rule_name = source.rule_name,
            target.severity = source.severity,
            target.total_rows = source.total_rows,
            target.failed_rows = source.failed_rows,
            target.status = source.status,
            target.action_taken = source.action_taken,
            target.details = source.details
        WHEN NOT MATCHED THEN INSERT (
            check_id,
            check_time,
            source_table,
            rule_name,
            severity,
            total_rows,
            failed_rows,
            status,
            action_taken,
            details
        )
        VALUES (
            source.check_id,
            source.check_time,
            source.source_table,
            source.rule_name,
            source.severity,
            source.total_rows,
            source.failed_rows,
            source.status,
            source.action_taken,
            source.details
        )
    """
    try:
        spark.sql(merge_sql)
        logger.info("Merged %s quality rule result rows.", len(results))
    finally:
        spark.catalog.dropTempView(view_name)


def merge_quarantine_candidates(
    spark: SparkSession,
    quarantine_candidates: List[DataFrame],
) -> None:
    if not quarantine_candidates:
        logger.info("No quarantine candidate rows to merge.")
        return

    union_df = reduce(lambda left, right: left.unionByName(right), quarantine_candidates)
    union_df = union_df.dropDuplicates(["quarantine_id"])
    candidate_count = union_df.count()

    view_name = "bronze_quarantine_candidates_src"
    union_df.createOrReplaceTempView(view_name)
    merge_sql = f"""
        MERGE INTO {QUARANTINE_TABLE} AS target
        USING {view_name} AS source
        ON target.quarantine_id = source.quarantine_id
        WHEN NOT MATCHED THEN INSERT (
            quarantine_id,
            detected_at,
            source_table,
            record_id,
            failed_rule,
            severity,
            failure_reason,
            raw_record,
            raw_payload,
            quarantine_status
        )
        VALUES (
            source.quarantine_id,
            source.detected_at,
            source.source_table,
            source.record_id,
            source.failed_rule,
            source.severity,
            source.failure_reason,
            source.raw_record,
            source.raw_payload,
            source.quarantine_status
        )
    """
    try:
        spark.sql(merge_sql)
        logger.info(
            "Merged quarantine candidate rows with insert-only semantics. "
            "quarantine_candidate_rows=%s",
            candidate_count,
        )
    finally:
        spark.catalog.dropTempView(view_name)


def check_learner_profiles(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = LEARNER_PROFILES_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    record_id_expr = build_record_id_expr(["user_id", "profile_updated_at"])
    parsed_df = df.withColumn("payload", F.from_json("raw_payload", LEARNER_PROFILES_PAYLOAD_SCHEMA))

    rules: List[Tuple[str, str, str, Callable[[], DataFrame]]] = [
        ("null_user_id", SEVERITY_FAIL, "Required field user_id is null.", lambda: df.filter(F.col("user_id").isNull())),
        ("null_profile_updated_at", SEVERITY_FAIL, "Required field profile_updated_at is null.", lambda: df.filter(F.col("profile_updated_at").isNull())),
        ("null_ingestion_time", SEVERITY_FAIL, "Required field ingestion_time is null.", lambda: df.filter(F.col("ingestion_time").isNull())),
        ("null_source_system", SEVERITY_FAIL, "Required field source_system is null.", lambda: df.filter(F.col("source_system").isNull())),
        ("empty_raw_payload", SEVERITY_FAIL, "raw_payload is null or blank.", lambda: df.filter(F.col("raw_payload").isNull() | (F.trim(F.col("raw_payload")) == ""))),
        ("duplicate_business_key_user_id_profile_updated_at", SEVERITY_FAIL, "Duplicate user_id + profile_updated_at business key.", lambda: df.withColumn("dup_count", F.count(F.lit(1)).over(Window.partitionBy("user_id", "profile_updated_at"))).filter(F.col("dup_count") > 1).drop("dup_count")),
        ("invalid_json_raw_payload", SEVERITY_FAIL, "raw_payload JSON does not match learner_profiles schema.", lambda: parsed_df.filter(F.col("payload").isNull()).drop("payload")),
        ("invalid_timestamp_order_ingestion_vs_profile_updated_at", SEVERITY_FAIL, "ingestion_time is earlier than profile_updated_at.", lambda: df.filter(F.col("ingestion_time") < F.col("profile_updated_at"))),
    ]

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    has_critical_failure = False

    for rule_name, severity, details, failing_builder in rules:
        result, quarantine_df, critical = evaluate_rule(
            source_df=df,
            source_table=source_table,
            total_rows=total_rows,
            rule_name=rule_name,
            severity=severity,
            details=details,
            failing_df_builder=failing_builder,
            record_id_expr=record_id_expr,
        )
        results.append(result)
        if quarantine_df is not None:
            quarantine_candidates.append(quarantine_df)
        has_critical_failure = has_critical_failure or critical

    return results, quarantine_candidates, has_critical_failure


def check_question_bank(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = QUESTION_BANK_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    record_id_expr = build_record_id_expr(["question_id", "question_version"])
    parsed_df = df.withColumn("payload", F.from_json("raw_payload", QUESTION_BANK_PAYLOAD_SCHEMA))

    rules: List[Tuple[str, str, str, Callable[[], DataFrame]]] = [
        ("null_question_id", SEVERITY_FAIL, "Required field question_id is null.", lambda: df.filter(F.col("question_id").isNull())),
        ("null_question_version", SEVERITY_FAIL, "Required field question_version is null.", lambda: df.filter(F.col("question_version").isNull())),
        ("null_source_system", SEVERITY_FAIL, "Required field source_system is null.", lambda: df.filter(F.col("source_system").isNull())),
        ("null_created_at", SEVERITY_FAIL, "Required field created_at is null.", lambda: df.filter(F.col("created_at").isNull())),
        ("null_ingestion_time", SEVERITY_FAIL, "Required field ingestion_time is null.", lambda: df.filter(F.col("ingestion_time").isNull())),
        ("empty_raw_payload", SEVERITY_FAIL, "raw_payload is null or blank.", lambda: df.filter(F.col("raw_payload").isNull() | (F.trim(F.col("raw_payload")) == ""))),
        ("duplicate_business_key_question_id_question_version", SEVERITY_FAIL, "Duplicate question_id + question_version business key.", lambda: df.withColumn("dup_count", F.count(F.lit(1)).over(Window.partitionBy("question_id", "question_version"))).filter(F.col("dup_count") > 1).drop("dup_count")),
        ("invalid_json_raw_payload", SEVERITY_FAIL, "raw_payload JSON does not match question_bank schema.", lambda: parsed_df.filter(F.col("payload").isNull()).drop("payload")),
        ("invalid_question_version", SEVERITY_FAIL, "question_version must be > 0.", lambda: df.filter(F.col("question_version") <= 0)),
        ("invalid_timestamp_order_ingestion_vs_created_at", SEVERITY_FAIL, "ingestion_time is earlier than created_at.", lambda: df.filter(F.col("ingestion_time") < F.col("created_at"))),
        ("invalid_correct_option", SEVERITY_FAIL, "correct_option_letter must be one of A/B/C/D.", lambda: parsed_df.filter(F.col("payload.correct_option_letter").isNull() | ~F.col("payload.correct_option_letter").isin("A", "B", "C", "D")).drop("payload")),
        ("missing_question_text", SEVERITY_FAIL, "question_text is null or blank.", lambda: parsed_df.filter(F.col("payload.question_text").isNull() | (F.trim(F.col("payload.question_text")) == "")).drop("payload")),
        ("missing_all_four_options", SEVERITY_FAIL, "All option_a_text/option_b_text/option_c_text/option_d_text must be present.", lambda: parsed_df.filter(F.col("payload.option_a_text").isNull() | F.col("payload.option_b_text").isNull() | F.col("payload.option_c_text").isNull() | F.col("payload.option_d_text").isNull()).drop("payload")),
        ("question_difficulty_outside_1_5", SEVERITY_WARNING, "difficulty_level must be between 1 and 5.", lambda: parsed_df.filter(F.col("payload.difficulty_level").isNull() | ~F.col("payload.difficulty_level").between(1, 5)).drop("payload")),
    ]

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    has_critical_failure = False

    for rule_name, severity, details, failing_builder in rules:
        result, quarantine_df, critical = evaluate_rule(
            source_df=df,
            source_table=source_table,
            total_rows=total_rows,
            rule_name=rule_name,
            severity=severity,
            details=details,
            failing_df_builder=failing_builder,
            record_id_expr=record_id_expr,
        )
        results.append(result)
        if quarantine_df is not None:
            quarantine_candidates.append(quarantine_df)
        has_critical_failure = has_critical_failure or critical

    return results, quarantine_candidates, has_critical_failure


def check_reference_materials(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = REFERENCE_MATERIALS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    record_id_expr = build_record_id_expr(["reference_id"])
    parsed_df = df.withColumn("payload", F.from_json("raw_payload", REFERENCE_MATERIALS_PAYLOAD_SCHEMA))

    rules: List[Tuple[str, str, str, Callable[[], DataFrame]]] = [
        ("null_reference_id", SEVERITY_FAIL, "Required field reference_id is null.", lambda: df.filter(F.col("reference_id").isNull())),
        ("null_batch_id", SEVERITY_FAIL, "Required field batch_id is null.", lambda: df.filter(F.col("batch_id").isNull())),
        ("null_source_type", SEVERITY_FAIL, "Required field source_type is null.", lambda: df.filter(F.col("source_type").isNull())),
        ("null_source_name", SEVERITY_FAIL, "Required field source_name is null.", lambda: df.filter(F.col("source_name").isNull())),
        ("null_file_name", SEVERITY_FAIL, "Required field file_name is null.", lambda: df.filter(F.col("file_name").isNull())),
        ("null_import_time", SEVERITY_FAIL, "Required field import_time is null.", lambda: df.filter(F.col("import_time").isNull())),
        ("null_ingestion_time", SEVERITY_FAIL, "Required field ingestion_time is null.", lambda: df.filter(F.col("ingestion_time").isNull())),
        ("empty_raw_payload", SEVERITY_FAIL, "raw_payload is null or blank.", lambda: df.filter(F.col("raw_payload").isNull() | (F.trim(F.col("raw_payload")) == ""))),
        ("duplicate_reference_id", SEVERITY_FAIL, "Duplicate reference_id business key.", lambda: df.withColumn("dup_count", F.count(F.lit(1)).over(Window.partitionBy("reference_id"))).filter(F.col("dup_count") > 1).drop("dup_count")),
        ("invalid_json_raw_payload", SEVERITY_FAIL, "raw_payload JSON does not match reference_materials schema.", lambda: parsed_df.filter(F.col("payload").isNull()).drop("payload")),
        ("invalid_timestamp_order_ingestion_vs_import_time", SEVERITY_FAIL, "ingestion_time is earlier than import_time.", lambda: df.filter(F.col("ingestion_time") < F.col("import_time"))),
        ("empty_content_text", SEVERITY_WARNING, "content_text is null or blank.", lambda: parsed_df.filter(F.col("payload.content_text").isNull() | (F.trim(F.col("payload.content_text")) == "")).drop("payload")),
        ("empty_title", SEVERITY_WARNING, "title is null or blank.", lambda: parsed_df.filter(F.col("payload.title").isNull() | (F.trim(F.col("payload.title")) == "")).drop("payload")),
    ]

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    has_critical_failure = False

    for rule_name, severity, details, failing_builder in rules:
        result, quarantine_df, critical = evaluate_rule(
            source_df=df,
            source_table=source_table,
            total_rows=total_rows,
            rule_name=rule_name,
            severity=severity,
            details=details,
            failing_df_builder=failing_builder,
            record_id_expr=record_id_expr,
        )
        results.append(result)
        if quarantine_df is not None:
            quarantine_candidates.append(quarantine_df)
        has_critical_failure = has_critical_failure or critical

    return results, quarantine_candidates, has_critical_failure


def check_learning_feedback(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = LEARNING_FEEDBACK_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    record_id_expr = build_record_id_expr(["feedback_id"])
    parsed_df = df.withColumn("payload", F.from_json("raw_payload", LEARNING_FEEDBACK_PAYLOAD_SCHEMA))

    rules: List[Tuple[str, str, str, Callable[[], DataFrame]]] = [
        ("null_feedback_id", SEVERITY_FAIL, "Required field feedback_id is null.", lambda: df.filter(F.col("feedback_id").isNull())),
        ("null_user_id", SEVERITY_FAIL, "Required field user_id is null.", lambda: df.filter(F.col("user_id").isNull())),
        ("null_session_id", SEVERITY_FAIL, "Required field session_id is null.", lambda: df.filter(F.col("session_id").isNull())),
        ("null_feedback_stage", SEVERITY_FAIL, "Required field feedback_stage is null.", lambda: df.filter(F.col("feedback_stage").isNull())),
        ("null_feedback_time", SEVERITY_FAIL, "Required field feedback_time is null.", lambda: df.filter(F.col("feedback_time").isNull())),
        ("null_ingestion_time", SEVERITY_FAIL, "Required field ingestion_time is null.", lambda: df.filter(F.col("ingestion_time").isNull())),
        ("empty_raw_payload", SEVERITY_FAIL, "raw_payload is null or blank.", lambda: df.filter(F.col("raw_payload").isNull() | (F.trim(F.col("raw_payload")) == ""))),
        ("duplicate_feedback_id", SEVERITY_FAIL, "Duplicate feedback_id business key.", lambda: df.withColumn("dup_count", F.count(F.lit(1)).over(Window.partitionBy("feedback_id"))).filter(F.col("dup_count") > 1).drop("dup_count")),
        ("invalid_json_raw_payload", SEVERITY_FAIL, "raw_payload JSON does not match learning_feedback schema.", lambda: parsed_df.filter(F.col("payload").isNull()).drop("payload")),
        ("invalid_timestamp_order_ingestion_vs_feedback_time", SEVERITY_FAIL, "ingestion_time is earlier than feedback_time.", lambda: df.filter(F.col("ingestion_time") < F.col("feedback_time"))),
        ("invalid_feedback_stage", SEVERITY_FAIL, "feedback_stage must be before_practice, after_practice, or general_check_in.", lambda: df.filter(~F.col("feedback_stage").isin("before_practice", "after_practice", "general_check_in"))),
        ("missing_practice_id_before_after", SEVERITY_WARNING, "practice_id is required for before_practice/after_practice.", lambda: df.filter(F.col("feedback_stage").isin("before_practice", "after_practice") & F.col("practice_id").isNull())),
        ("unexpected_practice_id_general_check_in", SEVERITY_WARNING, "practice_id must be null for general_check_in.", lambda: df.filter((F.col("feedback_stage") == "general_check_in") & F.col("practice_id").isNotNull())),
        ("confidence_before_score_out_of_range", SEVERITY_WARNING, "confidence_before_score must be between 1 and 10 when present.", lambda: parsed_df.filter(F.col("payload.confidence_before_score").isNotNull() & ~F.col("payload.confidence_before_score").between(1, 10)).drop("payload")),
        ("confidence_after_score_out_of_range", SEVERITY_WARNING, "confidence_after_score must be between 1 and 10 when present.", lambda: parsed_df.filter(F.col("payload.confidence_after_score").isNotNull() & ~F.col("payload.confidence_after_score").between(1, 10)).drop("payload")),
        ("expected_difficulty_score_out_of_range", SEVERITY_WARNING, "expected_difficulty_score must be between 1 and 10 when present.", lambda: parsed_df.filter(F.col("payload.expected_difficulty_score").isNotNull() & ~F.col("payload.expected_difficulty_score").between(1, 10)).drop("payload")),
        ("perceived_difficulty_score_out_of_range", SEVERITY_WARNING, "perceived_difficulty_score must be between 1 and 10 when present.", lambda: parsed_df.filter(F.col("payload.perceived_difficulty_score").isNotNull() & ~F.col("payload.perceived_difficulty_score").between(1, 10)).drop("payload")),
        ("overall_confidence_score_out_of_range", SEVERITY_WARNING, "overall_confidence_score must be between 1 and 10 when present.", lambda: parsed_df.filter(F.col("payload.overall_confidence_score").isNotNull() & ~F.col("payload.overall_confidence_score").between(1, 10)).drop("payload")),
        ("overall_motivation_score_out_of_range", SEVERITY_WARNING, "overall_motivation_score must be between 1 and 10 when present.", lambda: parsed_df.filter(F.col("payload.overall_motivation_score").isNotNull() & ~F.col("payload.overall_motivation_score").between(1, 10)).drop("payload")),
        ("overall_stress_score_out_of_range", SEVERITY_WARNING, "overall_stress_score must be between 1 and 10 when present.", lambda: parsed_df.filter(F.col("payload.overall_stress_score").isNotNull() & ~F.col("payload.overall_stress_score").between(1, 10)).drop("payload")),
    ]

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    has_critical_failure = False

    for rule_name, severity, details, failing_builder in rules:
        result, quarantine_df, critical = evaluate_rule(
            source_df=df,
            source_table=source_table,
            total_rows=total_rows,
            rule_name=rule_name,
            severity=severity,
            details=details,
            failing_df_builder=failing_builder,
            record_id_expr=record_id_expr,
        )
        results.append(result)
        if quarantine_df is not None:
            quarantine_candidates.append(quarantine_df)
        has_critical_failure = has_critical_failure or critical

    return results, quarantine_candidates, has_critical_failure


def check_learning_events(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = LEARNING_EVENTS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    record_id_expr = build_record_id_expr(["event_id"])
    parsed_df = df.withColumn("payload", F.from_json("raw_payload", LEARNING_EVENTS_PAYLOAD_SCHEMA))

    rules: List[Tuple[str, str, str, Callable[[], DataFrame]]] = [
        ("null_event_id", SEVERITY_FAIL, "Required field event_id is null.", lambda: df.filter(F.col("event_id").isNull())),
        ("null_user_id", SEVERITY_FAIL, "Required field user_id is null.", lambda: df.filter(F.col("user_id").isNull())),
        ("null_session_id", SEVERITY_FAIL, "Required field session_id is null.", lambda: df.filter(F.col("session_id").isNull())),
        ("null_event_type", SEVERITY_FAIL, "Required field event_type is null.", lambda: df.filter(F.col("event_type").isNull())),
        ("null_event_time", SEVERITY_FAIL, "Required field event_time is null.", lambda: df.filter(F.col("event_time").isNull())),
        ("null_ingestion_time", SEVERITY_FAIL, "Required field ingestion_time is null.", lambda: df.filter(F.col("ingestion_time").isNull())),
        ("null_source_system", SEVERITY_FAIL, "Required field source_system is null.", lambda: df.filter(F.col("source_system").isNull())),
        ("empty_raw_payload", SEVERITY_FAIL, "raw_payload is null or blank.", lambda: df.filter(F.col("raw_payload").isNull() | (F.trim(F.col("raw_payload")) == ""))),
        ("duplicate_event_id", SEVERITY_FAIL, "Duplicate event_id business key.", lambda: df.withColumn("dup_count", F.count(F.lit(1)).over(Window.partitionBy("event_id"))).filter(F.col("dup_count") > 1).drop("dup_count")),
        ("invalid_json_raw_payload", SEVERITY_FAIL, "raw_payload JSON does not match learning_events schema.", lambda: parsed_df.filter(F.col("payload").isNull()).drop("payload")),
        ("invalid_timestamp_order_ingestion_vs_event_time", SEVERITY_FAIL, "ingestion_time is earlier than event_time.", lambda: df.filter(F.col("ingestion_time") < F.col("event_time"))),
        ("invalid_event_type", SEVERITY_FAIL, "event_type must be ai_learning_interaction or practice_submitted.", lambda: df.filter(~F.col("event_type").isin("ai_learning_interaction", "practice_submitted"))),
        ("source_system_mismatch_by_event_type", SEVERITY_WARNING, "source_system must match expected value for each event_type.", lambda: df.filter(((F.col("event_type") == "ai_learning_interaction") & (F.col("source_system") != "chat")) | ((F.col("event_type") == "practice_submitted") & (F.col("source_system") != "practice_app")))),
    ]

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    has_critical_failure = False

    for rule_name, severity, details, failing_builder in rules:
        result, quarantine_df, critical = evaluate_rule(
            source_df=df,
            source_table=source_table,
            total_rows=total_rows,
            rule_name=rule_name,
            severity=severity,
            details=details,
            failing_df_builder=failing_builder,
            record_id_expr=record_id_expr,
        )
        results.append(result)
        if quarantine_df is not None:
            quarantine_candidates.append(quarantine_df)
        has_critical_failure = has_critical_failure or critical

    return results, quarantine_candidates, has_critical_failure


def run_job(spark: SparkSession) -> int:
    logger.info("Bronze quality job started.")
    execution_identity = get_execution_identity()
    logger.info("Execution identity: %s", execution_identity)

    all_results: List[RuleOutput] = []
    all_quarantine_candidates: List[DataFrame] = []
    critical_fail_detected = False

    table_checkers = [
        check_learner_profiles,
        check_question_bank,
        check_reference_materials,
        check_learning_feedback,
        check_learning_events,
    ]

    for checker in table_checkers:
        results, quarantine_candidates, has_critical_failure = checker(spark)
        all_results.extend(results)
        all_quarantine_candidates.extend(quarantine_candidates)
        critical_fail_detected = critical_fail_detected or has_critical_failure

    check_time = datetime.now(timezone.utc)
    merge_quality_results(
        spark=spark,
        execution_identity=execution_identity,
        check_time=check_time,
        results=all_results,
    )
    merge_quarantine_candidates(spark=spark, quarantine_candidates=all_quarantine_candidates)

    pass_count = sum(1 for result in all_results if result.status == STATUS_PASS)
    warning_count = sum(1 for result in all_results if result.status == STATUS_WARNING)
    fail_count = sum(1 for result in all_results if result.status == STATUS_FAIL)
    logger.info(
        "Final rule status counts | PASS=%s WARNING=%s FAIL=%s",
        pass_count,
        warning_count,
        fail_count,
    )

    if critical_fail_detected:
        logger.error("Critical FAIL rules detected with failed_rows > 0. Exiting with code 1.")
        return 1

    logger.info("No critical FAIL rules detected. Exiting with code 0.")
    return 0


def main() -> int:
    spark: Optional[SparkSession] = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("Unexpected error while running bronze quality job.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
