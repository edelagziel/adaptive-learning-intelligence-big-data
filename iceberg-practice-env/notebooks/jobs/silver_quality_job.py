#!/usr/bin/env python
# coding: utf-8
"""Recurring Silver quality job using idempotent Iceberg MERGE writes.

The job reads existing Silver tables, evaluates predefined quality rules, writes
one result row per rule, and writes audit-only quarantine copies for row-level
failures. Original Silver rows are never changed by this job.
"""

from __future__ import annotations

import hashlib
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from typing import Callable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Silver tables
# ---------------------------------------------------------------------------
LEARNER_PROFILES_TABLE = "demo.silver.learner_profiles"
QUESTION_BANK_TABLE = "demo.silver.question_bank"
REFERENCE_MATERIALS_TABLE = "demo.silver.reference_materials"
LEARNING_EVENTS_TABLE = "demo.silver.learning_events"
PRACTICE_ATTEMPTS_TABLE = "demo.silver.practice_attempts"
PRE_PRACTICE_FEEDBACK_TABLE = "demo.silver.pre_practice_feedback"
POST_PRACTICE_FEEDBACK_TABLE = "demo.silver.post_practice_feedback"
LEARNER_CHECK_IN_TABLE = "demo.silver.learner_check_in"
LEARNER_CHECK_IN_TOPICS_TABLE = "demo.silver.learner_check_in_topics"
AI_EXTRACTED_INSIGHTS_TABLE = "demo.silver.ai_extracted_insights"
VALIDATED_LEARNING_INSIGHTS_TABLE = "demo.silver.validated_learning_insights"
CONTENT_TAXONOMY_TABLE = "demo.silver.content_taxonomy"
LEARNER_CONCEPT_EVIDENCE_TABLE = "demo.silver.learner_concept_evidence"

QUALITY_RESULTS_TABLE = "demo.quality.silver_quality_results"
QUARANTINE_TABLE = "demo.quality.silver_quarantine"
BRONZE_LEARNING_EVENTS_TABLE = "demo.bronze.learning_events"
BRONZE_QUARANTINE_TABLE = "demo.quality.bronze_quarantine"

STATUS_PASS = "PASS"
STATUS_WARNING = "WARNING"
STATUS_FAIL = "FAIL"

SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_WARNING = "WARNING"

ACTION_NONE = "none"
ACTION_LOGGED_WARNING = "logged_warning"
ACTION_QUARANTINE_WARNING = "copied_to_quarantine_warning"
ACTION_QUARANTINE_FAIL = "copied_to_quarantine_fail"
ACTION_PIPELINE_BLOCKED = "pipeline_blocked"

OPEN_STATUS = "OPEN"
NULL_PLACEHOLDER = "__NULL__"

EXPECTED_DATA_TABLES = [
    LEARNER_PROFILES_TABLE,
    QUESTION_BANK_TABLE,
    REFERENCE_MATERIALS_TABLE,
    LEARNING_EVENTS_TABLE,
    PRACTICE_ATTEMPTS_TABLE,
    PRE_PRACTICE_FEEDBACK_TABLE,
    POST_PRACTICE_FEEDBACK_TABLE,
    LEARNER_CHECK_IN_TABLE,
    LEARNER_CHECK_IN_TOPICS_TABLE,
    AI_EXTRACTED_INSIGHTS_TABLE,
    VALIDATED_LEARNING_INSIGHTS_TABLE,
    CONTENT_TAXONOMY_TABLE,
    LEARNER_CONCEPT_EVIDENCE_TABLE,
]


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
    return SparkSession.builder.appName("silver_quality_job").getOrCreate()


def deterministic_check_id(source_table: str, rule_name: str) -> str:
    raw = f"{source_table}|{rule_name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def status_for(failed_rows: int, severity: str) -> str:
    if failed_rows == 0:
        return STATUS_PASS
    if severity == SEVERITY_WARNING:
        return STATUS_WARNING
    return STATUS_FAIL


def action_for(failed_rows: int, severity: str, supports_quarantine: bool) -> str:
    if failed_rows == 0:
        return ACTION_NONE
    if supports_quarantine and severity == SEVERITY_WARNING:
        return ACTION_QUARANTINE_WARNING
    if supports_quarantine:
        return ACTION_QUARANTINE_FAIL
    if severity == SEVERITY_WARNING:
        return ACTION_LOGGED_WARNING
    return ACTION_PIPELINE_BLOCKED


def is_blank(column_name: str) -> F.Column:
    return F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == "")


def build_record_id_expr(columns: Sequence[str]) -> F.Column:
    return F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit(NULL_PLACEHOLDER))
            for column in columns
        ],
    )


def bronze_learning_events_source_df(spark: SparkSession) -> DataFrame:
    source_df = spark.table(BRONZE_LEARNING_EVENTS_TABLE)
    scoped_count = source_df.count()
    open_quarantine_df = (
        spark.table(BRONZE_QUARANTINE_TABLE)
        .filter(
            (F.col("source_table") == BRONZE_LEARNING_EVENTS_TABLE)
            & (F.col("quarantine_status") == OPEN_STATUS)
        )
        .select(F.col("record_id").alias("_quality_record_id"))
        .distinct()
    )
    clean_df = (
        source_df
        .withColumn("_quality_record_id", build_record_id_expr(["event_id"]))
        .join(open_quarantine_df, "_quality_record_id", "left_anti")
        .drop("_quality_record_id")
    )
    clean_count = clean_df.count()
    logger.info(
        "Bronze quarantine exclusion for Silver quality | table=%s scoped_rows=%s open_quarantined_rows=%s valid_rows=%s",
        BRONZE_LEARNING_EVENTS_TABLE,
        scoped_count,
        scoped_count - clean_count,
        clean_count,
    )
    if scoped_count > 0 and clean_count == 0:
        logger.warning(
            "Bronze quarantine exclusion left zero valid rows for Silver quality | table=%s",
            BRONZE_LEARNING_EVENTS_TABLE,
        )
    return clean_df


def duplicate_rows(df: DataFrame, keys: Sequence[str]) -> DataFrame:
    duplicate_keys_df = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .select(*keys)
    )
    return df.join(duplicate_keys_df, list(keys), "inner").select(df["*"])


def build_quarantine_candidates(
    failed_df: DataFrame,
    source_df: DataFrame,
    source_table: str,
    rule_name: str,
    severity: str,
    failure_reason: str,
    record_id_expr: F.Column,
) -> DataFrame:
    source_columns = source_df.columns
    raw_payload_expr = (
        F.col("raw_payload").cast("string")
        if "raw_payload" in source_columns
        else F.lit(None).cast("string")
    )

    with_record_id = failed_df.withColumn("record_id", record_id_expr)
    return (
        with_record_id
        .withColumn("source_table", F.lit(source_table))
        .withColumn("failed_rule", F.lit(rule_name))
        .withColumn("severity", F.lit(severity))
        .withColumn("failure_reason", F.lit(failure_reason))
        .withColumn("raw_record", F.to_json(F.struct(*[F.col(c) for c in source_columns])))
        .withColumn("raw_record_hash", F.sha2(F.col("raw_record"), 256))
        .withColumn("quarantine_status", F.lit(OPEN_STATUS))
        .withColumn("raw_payload", raw_payload_expr)
        .withColumn(
            "quarantine_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("source_table"),
                    F.col("failed_rule"),
                    F.col("record_id"),
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
    record_id_columns: Sequence[str],
    supports_quarantine: bool = True,
) -> Tuple[RuleOutput, Optional[DataFrame], bool]:
    logger.info("Starting check | table=%s rule=%s", source_table, rule_name)
    failed_df = failing_df_builder()
    failed_rows = failed_df.count()
    status = status_for(failed_rows, severity)
    action_taken = action_for(failed_rows, severity, supports_quarantine)
    has_critical_failure = (
        severity == SEVERITY_CRITICAL
        and failed_rows > 0
        and not supports_quarantine
    )

    quarantine_df = None
    quarantine_count = 0
    if supports_quarantine and failed_rows > 0:
        quarantine_df = build_quarantine_candidates(
            failed_df=failed_df,
            source_df=source_df,
            source_table=source_table,
            rule_name=rule_name,
            severity=severity,
            failure_reason=details,
            record_id_expr=build_record_id_expr(record_id_columns),
        )
        quarantine_count = quarantine_df.count()

    logger.info(
        "Rule evaluated | table=%s rule=%s total_rows=%s failed_rows=%s status=%s quarantine_candidate_count=%s",
        source_table,
        rule_name,
        total_rows,
        failed_rows,
        status,
        quarantine_count,
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


def evaluate_aggregate_rule(
    source_table: str,
    total_rows: int,
    rule_name: str,
    severity: str,
    failed_rows: int,
    details: str,
) -> Tuple[RuleOutput, None, bool]:
    logger.info("Starting check | table=%s rule=%s", source_table, rule_name)
    status = status_for(failed_rows, severity)
    action_taken = action_for(failed_rows, severity, supports_quarantine=False)
    has_critical_failure = severity == SEVERITY_CRITICAL and failed_rows > 0
    logger.info(
        "Rule evaluated | table=%s rule=%s total_rows=%s failed_rows=%s status=%s quarantine_candidate_count=0",
        source_table,
        rule_name,
        total_rows,
        failed_rows,
        status,
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
        None,
        has_critical_failure,
    )


def merge_quality_results(
    spark: SparkSession,
    check_time: datetime,
    results: List[RuleOutput],
) -> None:
    if not results:
        logger.info("No quality results to merge.")
        return

    rows = [
        (
            deterministic_check_id(result.source_table, result.rule_name),
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

    view_name = "silver_quality_results_src"
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
    spark.sql(merge_sql)
    logger.info("Merged %s silver quality result rows.", len(results))


def merge_quarantine_candidates(
    spark: SparkSession,
    quarantine_candidates: List[DataFrame],
) -> None:
    if not quarantine_candidates:
        logger.info("No silver quarantine candidate rows to merge.")
        return

    union_df = reduce(lambda left, right: left.unionByName(right), quarantine_candidates)
    quarantine_columns = union_df.columns
    union_df = union_df.groupBy("quarantine_id").agg(
        *[
            F.first(column, ignorenulls=False).alias(column)
            for column in quarantine_columns
            if column != "quarantine_id"
        ]
    )
    candidate_count = union_df.count()

    view_name = "silver_quarantine_candidates_src"
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
    spark.sql(merge_sql)
    logger.info("Merged silver quarantine candidate rows: %s", candidate_count)


def add_empty_table_warning(
    df: DataFrame,
    source_table: str,
    total_rows: int,
) -> Tuple[RuleOutput, None, bool]:
    return evaluate_aggregate_rule(
        source_table=source_table,
        total_rows=total_rows,
        rule_name="empty_table_warning",
        severity=SEVERITY_WARNING,
        failed_rows=1 if total_rows == 0 else 0,
        details="Expected Silver table contains no rows.",
    )


def collect_rule(
    output: Tuple[RuleOutput, Optional[DataFrame], bool],
    results: List[RuleOutput],
    quarantine_candidates: List[DataFrame],
) -> bool:
    result, quarantine_df, critical = output
    results += [result]
    if quarantine_df is not None:
        quarantine_candidates += [quarantine_df]
    return critical


def check_learner_profiles(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = LEARNER_PROFILES_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)

    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required learner profile fields are null or blank.",
            lambda: df.filter(
                is_blank("user_id")
                | F.col("registration_date").isNull()
                | F.col("profile_updated_at").isNull()
                | F.col("ingestion_time").isNull()
            ),
            ["user_id", "profile_updated_at"],
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "registration_after_profile_update",
            SEVERITY_CRITICAL,
            "registration_date is after profile_updated_at date.",
            lambda: df.filter(F.col("registration_date") > F.to_date(F.col("profile_updated_at"))),
            ["user_id", "profile_updated_at"],
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "ingestion_before_profile_update",
            SEVERITY_CRITICAL,
            "ingestion_time is earlier than profile_updated_at.",
            lambda: df.filter(F.col("ingestion_time") < F.col("profile_updated_at")),
            ["user_id", "profile_updated_at"],
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_user_id_profile_updated_at",
            SEVERITY_CRITICAL,
            "Duplicate user_id + profile_updated_at grain.",
            lambda: duplicate_rows(df, ["user_id", "profile_updated_at"]),
            ["user_id", "profile_updated_at"],
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "invalid_current_profile_count_per_user",
            SEVERITY_CRITICAL,
            "Each user must have exactly one current profile.",
            lambda: df.join(
                df.groupBy("user_id")
                .agg(F.sum(F.when(F.col("is_current") == True, F.lit(1)).otherwise(F.lit(0))).alias("current_profile_count"))
                .filter(F.col("current_profile_count") != 1)
                .select("user_id"),
                ["user_id"],
                "inner",
            ),
            ["user_id", "profile_updated_at"],
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_question_bank(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = QUESTION_BANK_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["question_id", "question_version"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required question-bank fields are null or blank.",
            lambda: df.filter(
                is_blank("question_id")
                | F.col("question_version").isNull()
                | is_blank("question_text")
                | is_blank("option_a_text")
                | is_blank("option_b_text")
                | is_blank("option_c_text")
                | is_blank("option_d_text")
                | is_blank("correct_option_letter")
                | F.col("difficulty_level").isNull()
                | is_blank("validation_status")
                | is_blank("content_hash")
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    for rule_name, details, condition in [
        ("invalid_question_version", "question_version must be positive.", F.col("question_version").isNull() | (F.col("question_version") <= 0)),
        ("invalid_correct_option", "correct_option_letter must be A, B, C, or D.", F.col("correct_option_letter").isNull() | ~F.col("correct_option_letter").isin("A", "B", "C", "D")),
        ("invalid_difficulty_level", "difficulty_level must be between 1 and 5.", F.col("difficulty_level").isNull() | ~F.col("difficulty_level").between(1, 5)),
        ("invalid_validation_status", "validation_status must be pending, approved, rejected, or flagged.", F.col("validation_status").isNull() | ~F.col("validation_status").isin("pending", "approved", "rejected", "flagged")),
    ]:
        critical |= collect_rule(
            evaluate_rule(
                df,
                source_table,
                total_rows,
                rule_name,
                SEVERITY_CRITICAL,
                details,
                lambda condition=condition: df.filter(condition),
                key_cols,
            ),
            results,
            quarantine_candidates,
        )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_question_id_question_version",
            SEVERITY_CRITICAL,
            "Duplicate question_id + question_version grain.",
            lambda: duplicate_rows(df, key_cols),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_content_hash",
            SEVERITY_WARNING,
            "Duplicate question content_hash values.",
            lambda: duplicate_rows(df, ["content_hash"]),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_reference_materials(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = REFERENCE_MATERIALS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["reference_id"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required reference material fields are null or blank.",
            lambda: df.filter(
                is_blank("reference_id")
                | is_blank("source_type")
                | is_blank("title")
                | is_blank("content_text")
                | F.col("import_time").isNull()
                | F.col("ingestion_time").isNull()
                | is_blank("content_hash")
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    for rule_name, details, condition in [
        ("invalid_reliability_level", "reliability_level must be official, approved, or external.", F.col("reliability_level").isNull() | ~F.col("reliability_level").isin("official", "approved", "external")),
        ("ingestion_before_import_time", "ingestion_time is earlier than import_time.", F.col("ingestion_time") < F.col("import_time")),
    ]:
        critical |= collect_rule(
            evaluate_rule(
                df,
                source_table,
                total_rows,
                rule_name,
                SEVERITY_CRITICAL,
                details,
                lambda condition=condition: df.filter(condition),
                key_cols,
            ),
            results,
            quarantine_candidates,
        )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_reference_id",
            SEVERITY_CRITICAL,
            "Duplicate reference_id grain.",
            lambda: duplicate_rows(df, key_cols),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_content_hash",
            SEVERITY_WARNING,
            "Duplicate reference content_hash values.",
            lambda: duplicate_rows(df, ["content_hash"]),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_learning_events(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = LEARNING_EVENTS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["event_id"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required learning event fields are null or blank.",
            lambda: df.filter(
                is_blank("event_id")
                | is_blank("user_id")
                | is_blank("session_id")
                | is_blank("event_type")
                | F.col("event_time").isNull()
                | F.col("ingestion_time").isNull()
                | is_blank("source_system")
                | F.col("event_date").isNull()
                | F.col("event_hour").isNull()
                | F.col("payload_valid").isNull()
                | is_blank("processing_status")
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    rule_conditions = [
        ("invalid_event_type", "event_type must be ai_learning_interaction for demo.silver.learning_events.", F.col("event_type").isNull() | (F.col("event_type") != "ai_learning_interaction"), SEVERITY_CRITICAL),
        ("invalid_event_date", "event_date must equal to_date(event_time).", F.col("event_date") != F.to_date(F.col("event_time")), SEVERITY_CRITICAL),
        ("invalid_event_hour", "event_hour must equal hour(event_time).", F.col("event_hour") != F.hour(F.col("event_time")), SEVERITY_CRITICAL),
        ("payload_not_valid", "payload_valid must be true.", F.col("payload_valid") != True, SEVERITY_CRITICAL),
        ("processing_not_processed", "processing_status must be processed.", F.col("processing_status") != "processed", SEVERITY_CRITICAL),
        ("ingestion_before_event_time", "ingestion_time is earlier than event_time.", F.col("ingestion_time") < F.col("event_time"), SEVERITY_CRITICAL),
        ("source_system_mismatch_by_event_type", "source_system does not match ai_learning_interaction expectation.", (F.col("event_type") == "ai_learning_interaction") & (F.col("source_system") != "chat"), SEVERITY_WARNING),
    ]
    for rule_name, details, condition, severity in rule_conditions:
        critical |= collect_rule(
            evaluate_rule(
                df,
                source_table,
                total_rows,
                rule_name,
                severity,
                details,
                lambda condition=condition: df.filter(condition),
                key_cols,
            ),
            results,
            quarantine_candidates,
        )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_event_id",
            SEVERITY_CRITICAL,
            "Duplicate event_id grain.",
            lambda: duplicate_rows(df, key_cols),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_practice_attempts(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = PRACTICE_ATTEMPTS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    learning_events_df = bronze_learning_events_source_df(spark)
    question_bank_df = spark.table(QUESTION_BANK_TABLE)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["attempt_id"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required practice attempt fields are null or blank.",
            lambda: df.filter(
                is_blank("attempt_id")
                | is_blank("event_id")
                | is_blank("user_id")
                | is_blank("practice_id")
                | is_blank("question_id")
                | F.col("question_version").isNull()
                | is_blank("selected_option_letter")
                | F.col("is_correct").isNull()
                | F.col("score").isNull()
                | F.col("hints_used").isNull()
                | F.col("attempt_duration_seconds").isNull()
                | F.col("attempt_number").isNull()
                | F.col("attempt_time").isNull()
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    rule_conditions = [
        ("invalid_selected_option", "selected_option_letter must be A, B, C, or D.", F.col("selected_option_letter").isNull() | ~F.col("selected_option_letter").isin("A", "B", "C", "D")),
        ("invalid_score", "score must be 0.0 or 1.0.", F.col("score").isNull() | ~F.col("score").isin(0.0, 1.0)),
        ("negative_hints_used", "hints_used must be non-negative.", F.col("hints_used").isNull() | (F.col("hints_used") < 0)),
        ("negative_attempt_duration", "attempt_duration_seconds must be non-negative.", F.col("attempt_duration_seconds").isNull() | (F.col("attempt_duration_seconds") < 0)),
        ("invalid_attempt_number", "attempt_number must be positive.", F.col("attempt_number").isNull() | (F.col("attempt_number") <= 0)),
    ]
    for rule_name, details, condition in rule_conditions:
        critical |= collect_rule(
            evaluate_rule(
                df,
                source_table,
                total_rows,
                rule_name,
                SEVERITY_CRITICAL,
                details,
                lambda condition=condition: df.filter(condition),
                key_cols,
            ),
            results,
            quarantine_candidates,
        )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_attempt_id",
            SEVERITY_CRITICAL,
            "Duplicate attempt_id grain.",
            lambda: duplicate_rows(df, key_cols),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_or_wrong_event_fk",
            SEVERITY_CRITICAL,
            "practice_attempts.event_id must link to a non-quarantined Bronze practice_submitted learning event.",
            lambda: df.alias("pa").join(
                learning_events_df.alias("le"),
                F.col("pa.event_id") == F.col("le.event_id"),
                "left",
            ).filter(
                F.col("le.event_id").isNull()
                | (F.lower(F.trim(F.col("le.event_type"))) != "practice_submitted")
            ).select("pa.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_question_fk",
            SEVERITY_CRITICAL,
            "practice_attempts question key must link to question_bank.",
            lambda: df.alias("pa").join(
                question_bank_df.alias("qb"),
                (F.col("pa.question_id") == F.col("qb.question_id"))
                & (F.col("pa.question_version") == F.col("qb.question_version")),
                "left",
            ).filter(F.col("qb.question_id").isNull()).select("pa.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "incorrect_is_correct_or_score",
            SEVERITY_CRITICAL,
            "is_correct and score must match question_bank.correct_option_letter.",
            lambda: df.alias("pa").join(
                question_bank_df.alias("qb"),
                (F.col("pa.question_id") == F.col("qb.question_id"))
                & (F.col("pa.question_version") == F.col("qb.question_version")),
                "inner",
            ).filter(
                (F.col("pa.is_correct") != (F.col("pa.selected_option_letter") == F.col("qb.correct_option_letter")))
                | (
                    F.col("pa.score")
                    != F.when(F.col("pa.selected_option_letter") == F.col("qb.correct_option_letter"), F.lit(1.0)).otherwise(F.lit(0.0))
                )
            ).select("pa.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_feedback_table(
    spark: SparkSession,
    source_table: str,
    score_columns: Sequence[str],
    required_columns: Sequence[str],
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["feedback_id"]
    missing_condition = None
    for column in required_columns:
        condition = is_blank(column) if column.endswith("_id") or column == "free_text" else F.col(column).isNull()
        missing_condition = condition if missing_condition is None else missing_condition | condition

    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required feedback fields are null or blank.",
            lambda: df.filter(missing_condition),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "ingestion_before_feedback_time",
            SEVERITY_CRITICAL,
            "ingestion_time is earlier than feedback_time.",
            lambda: df.filter(F.col("ingestion_time") < F.col("feedback_time")),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "negative_delay_minutes",
            SEVERITY_CRITICAL,
            "delay_minutes must be non-negative.",
            lambda: df.filter(F.col("delay_minutes").isNull() | (F.col("delay_minutes") < 0)),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    score_condition = None
    for column in score_columns:
        condition = F.col(column).isNull() | ~F.col(column).between(1, 10)
        score_condition = condition if score_condition is None else score_condition | condition
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "score_out_of_range_1_10",
            SEVERITY_CRITICAL,
            "Feedback score fields must be between 1 and 10.",
            lambda: df.filter(score_condition),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_feedback_id",
            SEVERITY_CRITICAL,
            "Duplicate feedback_id grain.",
            lambda: duplicate_rows(df, key_cols),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_learner_check_in_topics(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = LEARNER_CHECK_IN_TOPICS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    parent_df = spark.table(LEARNER_CHECK_IN_TABLE)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["feedback_id", "topic_id"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required check-in topic fields are null or blank.",
            lambda: df.filter(
                is_blank("feedback_id")
                | is_blank("user_id")
                | is_blank("topic_id")
                | F.col("feedback_time").isNull()
                | F.col("perceived_understanding_score").isNull()
                | F.col("topic_confidence_score").isNull()
                | F.col("still_confused").isNull()
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "score_out_of_range_1_10",
            SEVERITY_CRITICAL,
            "Check-in topic scores must be between 1 and 10.",
            lambda: df.filter(
                F.col("perceived_understanding_score").isNull()
                | ~F.col("perceived_understanding_score").between(1, 10)
                | F.col("topic_confidence_score").isNull()
                | ~F.col("topic_confidence_score").between(1, 10)
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_feedback_id_topic_id",
            SEVERITY_CRITICAL,
            "Duplicate feedback_id + topic_id grain.",
            lambda: duplicate_rows(df, key_cols),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_or_mismatched_check_in_parent",
            SEVERITY_CRITICAL,
            "Check-in topic row must link to matching learner_check_in parent.",
            lambda: df.alias("topics").join(
                parent_df.alias("parent"),
                F.col("topics.feedback_id") == F.col("parent.feedback_id"),
                "left",
            ).filter(
                F.col("parent.feedback_id").isNull()
                | (F.col("topics.user_id") != F.col("parent.user_id"))
                | ~(F.col("topics.session_id").eqNullSafe(F.col("parent.session_id")))
            ).select("topics.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_ai_extracted_insights(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = AI_EXTRACTED_INSIGHTS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    events_df = spark.table(LEARNING_EVENTS_TABLE)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["insight_id"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required AI insight fields are null or blank.",
            lambda: df.filter(
                is_blank("insight_id")
                | is_blank("event_id")
                | is_blank("user_id")
                | is_blank("dynamic_concept_name")
                | F.col("extracted_at").isNull()
                | F.col("extraction_confidence").isNull()
                | is_blank("processing_model")
                | is_blank("validation_status")
                | is_blank("ai_attributes")
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    rule_conditions = [
        ("invalid_extraction_confidence", "extraction_confidence must be between 0 and 1.", F.col("extraction_confidence").isNull() | ~F.col("extraction_confidence").between(0.0, 1.0)),
        ("invalid_validation_status", "validation_status must be pending, validated, flagged, or rejected.", F.col("validation_status").isNull() | ~F.col("validation_status").isin("pending", "validated", "flagged", "rejected")),
    ]
    for rule_name, details, condition in rule_conditions:
        critical |= collect_rule(
            evaluate_rule(df, source_table, total_rows, rule_name, SEVERITY_CRITICAL, details, lambda condition=condition: df.filter(condition), key_cols),
            results,
            quarantine_candidates,
        )
    critical |= collect_rule(
        evaluate_rule(df, source_table, total_rows, "duplicate_insight_id", SEVERITY_CRITICAL, "Duplicate insight_id grain.", lambda: duplicate_rows(df, key_cols), key_cols),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_or_wrong_source_event_fk",
            SEVERITY_CRITICAL,
            "AI insight event_id must link to an ai_learning_interaction event.",
            lambda: df.alias("ai").join(
                events_df.alias("le"),
                F.col("ai.event_id") == F.col("le.event_id"),
                "left",
            ).filter(
                F.col("le.event_id").isNull()
                | (F.col("le.event_type") != "ai_learning_interaction")
            ).select("ai.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "duplicate_event_id_dynamic_concept_name",
            SEVERITY_CRITICAL,
            "Duplicate event_id + dynamic_concept_name grain.",
            lambda: duplicate_rows(df, ["event_id", "dynamic_concept_name"]),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_validated_learning_insights(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = VALIDATED_LEARNING_INSIGHTS_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    ai_df = spark.table(AI_EXTRACTED_INSIGHTS_TABLE)
    reference_df = spark.table(REFERENCE_MATERIALS_TABLE)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["validation_id"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required validated insight fields are null or blank.",
            lambda: df.filter(
                is_blank("validation_id")
                | is_blank("insight_id")
                | is_blank("reference_id")
                | F.col("validation_time").isNull()
                | F.col("semantic_match_score").isNull()
                | F.col("reliability_score").isNull()
                | F.col("contradiction_flag").isNull()
                | is_blank("validation_status")
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    rule_conditions = [
        ("invalid_semantic_match_score", "semantic_match_score must be between 0 and 1.", F.col("semantic_match_score").isNull() | ~F.col("semantic_match_score").between(0.0, 1.0)),
        ("invalid_reliability_score", "reliability_score must be between 0 and 1.", F.col("reliability_score").isNull() | ~F.col("reliability_score").between(0.0, 1.0)),
        ("null_contradiction_flag", "contradiction_flag must not be null.", F.col("contradiction_flag").isNull()),
        ("invalid_validation_status", "validation_status must be validated, weak_match, no_reference, or contradiction.", F.col("validation_status").isNull() | ~F.col("validation_status").isin("validated", "weak_match", "no_reference", "contradiction")),
    ]
    for rule_name, details, condition in rule_conditions:
        critical |= collect_rule(
            evaluate_rule(df, source_table, total_rows, rule_name, SEVERITY_CRITICAL, details, lambda condition=condition: df.filter(condition), key_cols),
            results,
            quarantine_candidates,
        )
    critical |= collect_rule(
        evaluate_rule(df, source_table, total_rows, "duplicate_insight_id", SEVERITY_CRITICAL, "validated_learning_insights must contain one selected validation per insight_id.", lambda: duplicate_rows(df, ["insight_id"]), key_cols),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_insight_fk",
            SEVERITY_CRITICAL,
            "validated insight must link to ai_extracted_insights.",
            lambda: df.alias("v").join(ai_df.alias("ai"), F.col("v.insight_id") == F.col("ai.insight_id"), "left").filter(F.col("ai.insight_id").isNull()).select("v.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_reference_fk",
            SEVERITY_CRITICAL,
            "validated insight must link to reference_materials.",
            lambda: df.alias("v").join(reference_df.alias("r"), F.col("v.reference_id") == F.col("r.reference_id"), "left").filter(F.col("r.reference_id").isNull()).select("v.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    return results, quarantine_candidates, critical


def check_content_taxonomy(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = CONTENT_TAXONOMY_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["taxonomy_id"]
    taxonomy_with_parent_df = (
        df.alias("child")
        .join(df.alias("parent"), F.col("child.parent_taxonomy_id") == F.col("parent.taxonomy_id"), "left")
        .select("child.*", F.col("parent.taxonomy_level").alias("parent_level"))
    )
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required taxonomy fields are null or blank.",
            lambda: df.filter(
                is_blank("taxonomy_id")
                | F.col("domain").isNull()
                | F.col("normalized_domain").isNull()
                | is_blank("taxonomy_level")
                | is_blank("source_type")
                | F.col("first_detected_at").isNull()
                | F.col("last_detected_at").isNull()
                | is_blank("validation_status")
                | F.col("is_active").isNull()
                | is_blank("taxonomy_hash")
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    for rule_name, details, builder in [
        ("duplicate_taxonomy_id", "Duplicate taxonomy_id grain.", lambda: duplicate_rows(df, ["taxonomy_id"])),
        ("duplicate_taxonomy_hash", "Duplicate taxonomy_hash values.", lambda: duplicate_rows(df, ["taxonomy_hash"])),
        ("invalid_taxonomy_level", "taxonomy_level must be domain, topic, subtopic, or concept.", lambda: df.filter(F.col("taxonomy_level").isNull() | ~F.col("taxonomy_level").isin("domain", "topic", "subtopic", "concept"))),
        ("invalid_validation_status", "validation_status must be approved, pending, flagged, or rejected.", lambda: df.filter(F.col("validation_status").isNull() | ~F.col("validation_status").isin("approved", "pending", "flagged", "rejected"))),
        ("missing_parent", "parent_taxonomy_id must reference an existing taxonomy row when present.", lambda: taxonomy_with_parent_df.filter(F.col("parent_taxonomy_id").isNotNull() & F.col("parent_level").isNull()).select(*df.columns)),
        ("invalid_parent_level", "taxonomy hierarchy parent level is invalid.", lambda: taxonomy_with_parent_df.filter(
            ((F.col("taxonomy_level") == "domain") & F.col("parent_taxonomy_id").isNotNull())
            | ((F.col("taxonomy_level") == "topic") & (F.col("parent_taxonomy_id").isNull() | (F.col("parent_level") != "domain")))
            | ((F.col("taxonomy_level") == "subtopic") & (F.col("parent_taxonomy_id").isNull() | (F.col("parent_level") != "topic")))
            | ((F.col("taxonomy_level") == "concept") & (F.col("parent_taxonomy_id").isNull() | ~F.col("parent_level").isin("topic", "subtopic")))
        ).select(*df.columns)),
    ]:
        critical |= collect_rule(
            evaluate_rule(df, source_table, total_rows, rule_name, SEVERITY_CRITICAL, details, builder, key_cols),
            results,
            quarantine_candidates,
        )
    return results, quarantine_candidates, critical


def check_learner_concept_evidence(
    spark: SparkSession,
) -> Tuple[List[RuleOutput], List[DataFrame], bool]:
    source_table = LEARNER_CONCEPT_EVIDENCE_TABLE
    logger.info("Checking table: %s", source_table)
    df = spark.table(source_table)
    taxonomy_df = spark.table(CONTENT_TAXONOMY_TABLE)
    total_rows = df.count()
    logger.info("Total rows | table=%s total_rows=%s", source_table, total_rows)

    results: List[RuleOutput] = []
    quarantine_candidates: List[DataFrame] = []
    critical = False
    key_cols = ["evidence_id"]
    valid_types = ["practice_attempt", "ai_insight", "validated_insight", "pre_feedback", "post_feedback", "check_in"]
    critical |= collect_rule(add_empty_table_warning(df, source_table, total_rows), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_required_fields",
            SEVERITY_CRITICAL,
            "Required evidence fields are null or blank.",
            lambda: df.filter(
                is_blank("evidence_id")
                | is_blank("user_id")
                | is_blank("taxonomy_id")
                | is_blank("evidence_type")
                | F.col("evidence_time").isNull()
                | is_blank("source_table")
                | F.col("processing_time").isNull()
            ),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    critical |= collect_rule(evaluate_rule(df, source_table, total_rows, "duplicate_evidence_id", SEVERITY_CRITICAL, "Duplicate evidence_id grain.", lambda: duplicate_rows(df, key_cols), key_cols), results, quarantine_candidates)
    critical |= collect_rule(evaluate_rule(df, source_table, total_rows, "invalid_evidence_type", SEVERITY_CRITICAL, "evidence_type is not recognized.", lambda: df.filter(F.col("evidence_type").isNull() | ~F.col("evidence_type").isin(*valid_types)), key_cols), results, quarantine_candidates)
    critical |= collect_rule(
        evaluate_rule(
            df,
            source_table,
            total_rows,
            "missing_taxonomy_fk",
            SEVERITY_CRITICAL,
            "taxonomy_id must link to content_taxonomy.",
            lambda: df.alias("e").join(taxonomy_df.select("taxonomy_id").alias("t"), F.col("e.taxonomy_id") == F.col("t.taxonomy_id"), "left_anti").select("e.*"),
            key_cols,
        ),
        results,
        quarantine_candidates,
    )
    type_required_condition = (
        ((F.col("evidence_type") == "practice_attempt") & (F.col("attempt_id").isNull() | F.col("event_id").isNull() | F.col("is_correct").isNull() | F.col("score").isNull() | F.col("hints_used").isNull() | F.col("attempt_duration_seconds").isNull() | F.col("attempt_number").isNull()))
        | ((F.col("evidence_type") == "ai_insight") & (F.col("insight_id").isNull() | F.col("event_id").isNull() | F.col("extraction_confidence").isNull()))
        | ((F.col("evidence_type") == "validated_insight") & (F.col("validation_id").isNull() | F.col("insight_id").isNull() | F.col("event_id").isNull() | F.col("extraction_confidence").isNull() | F.col("semantic_match_score").isNull() | F.col("reliability_score").isNull() | F.col("contradiction_flag").isNull()))
        | ((F.col("evidence_type") == "pre_feedback") & (F.col("feedback_id").isNull() | F.col("confidence_score").isNull() | F.col("perceived_understanding_score").isNull() | F.col("perceived_difficulty_score").isNull()))
        | ((F.col("evidence_type") == "post_feedback") & (F.col("feedback_id").isNull() | F.col("confidence_score").isNull() | F.col("perceived_understanding_score").isNull() | F.col("perceived_difficulty_score").isNull() | F.col("still_confused").isNull()))
        | ((F.col("evidence_type") == "check_in") & (F.col("feedback_id").isNull() | F.col("confidence_score").isNull() | F.col("perceived_understanding_score").isNull() | F.col("still_confused").isNull()))
    )
    critical |= collect_rule(evaluate_rule(df, source_table, total_rows, "missing_required_fields_by_evidence_type", SEVERITY_CRITICAL, "Evidence-type-specific required fields are missing.", lambda: df.filter(type_required_condition), key_cols), results, quarantine_candidates)
    invalid_ranges = (
        (F.col("score").isNotNull() & ((F.col("score") < 0) | (F.col("score") > 1)))
        | (F.col("hints_used").isNotNull() & (F.col("hints_used") < 0))
        | (F.col("attempt_duration_seconds").isNotNull() & (F.col("attempt_duration_seconds") < 0))
        | (F.col("attempt_number").isNotNull() & (F.col("attempt_number") < 1))
        | (F.col("extraction_confidence").isNotNull() & ((F.col("extraction_confidence") < 0) | (F.col("extraction_confidence") > 1)))
        | (F.col("semantic_match_score").isNotNull() & ((F.col("semantic_match_score") < 0) | (F.col("semantic_match_score") > 1)))
        | (F.col("reliability_score").isNotNull() & ((F.col("reliability_score") < 0) | (F.col("reliability_score") > 1)))
        | (F.col("confidence_score").isNotNull() & ((F.col("confidence_score") < 1) | (F.col("confidence_score") > 10)))
        | (F.col("perceived_understanding_score").isNotNull() & ((F.col("perceived_understanding_score") < 1) | (F.col("perceived_understanding_score") > 10)))
        | (F.col("perceived_difficulty_score").isNotNull() & ((F.col("perceived_difficulty_score") < 1) | (F.col("perceived_difficulty_score") > 10)))
    )
    critical |= collect_rule(evaluate_rule(df, source_table, total_rows, "invalid_numeric_ranges", SEVERITY_CRITICAL, "Evidence numeric values are outside valid ranges.", lambda: df.filter(invalid_ranges), key_cols), results, quarantine_candidates)
    return results, quarantine_candidates, critical


def run_job(spark: SparkSession) -> int:
    logger.info("Silver quality job started.")
    all_results: List[RuleOutput] = []
    all_quarantine_candidates: List[DataFrame] = []
    critical_fail_detected = False

    table_checkers = [
        check_learner_profiles,
        check_question_bank,
        check_reference_materials,
        check_learning_events,
        check_practice_attempts,
        lambda spark: check_feedback_table(
            spark,
            PRE_PRACTICE_FEEDBACK_TABLE,
            ["confidence_before_score", "perceived_understanding_before_score", "expected_difficulty_score"],
            ["feedback_id", "user_id", "practice_id", "feedback_time", "ingestion_time", "delay_minutes", "confidence_before_score", "perceived_understanding_before_score", "expected_difficulty_score"],
        ),
        lambda spark: check_feedback_table(
            spark,
            POST_PRACTICE_FEEDBACK_TABLE,
            ["confidence_after_score", "perceived_understanding_after_score", "perceived_difficulty_score"],
            ["feedback_id", "user_id", "practice_id", "feedback_time", "ingestion_time", "delay_minutes", "confidence_after_score", "perceived_understanding_after_score", "perceived_difficulty_score", "still_confused"],
        ),
        lambda spark: check_feedback_table(
            spark,
            LEARNER_CHECK_IN_TABLE,
            ["overall_confidence_score", "overall_motivation_score", "overall_stress_score"],
            ["feedback_id", "user_id", "feedback_time", "ingestion_time", "delay_minutes", "overall_confidence_score", "overall_motivation_score", "overall_stress_score"],
        ),
        check_learner_check_in_topics,
        check_ai_extracted_insights,
        check_validated_learning_insights,
        check_content_taxonomy,
        check_learner_concept_evidence,
    ]

    for checker in table_checkers:
        results, quarantine_candidates, has_critical_failure = checker(spark)
        all_results.extend(results)
        all_quarantine_candidates.extend(quarantine_candidates)
        critical_fail_detected = critical_fail_detected or has_critical_failure

    check_time = datetime.now(timezone.utc)
    merge_quality_results(spark=spark, check_time=check_time, results=all_results)
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

    if fail_count > 0:
        logger.warning(
            "Silver row-level CRITICAL rules were detected and written as quarantine audit copies. "
            "Successfully quarantined row-level failures are non-blocking for this job."
        )

    if critical_fail_detected:
        logger.error("Non-quarantinable critical Silver quality failure detected. Exiting with code 1.")
        return 1

    logger.info("No blocking Silver quality failures detected. Exiting with code 0.")
    return 0


def main() -> int:
    spark: Optional[SparkSession] = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("Unexpected error while running silver quality job.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
