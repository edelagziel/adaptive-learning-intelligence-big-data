#!/usr/bin/env python
# coding: utf-8
"""Validation-only Gold quality gate."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import Dict, List, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"

GOLD_PREFIX = "demo.gold."


@dataclass
class RuleResult:
    rule_group: str
    table_name: str
    rule_name: str
    total_rows: int
    failed_rows: int
    status: str


MINIMUM_COUNTS: Dict[str, int] = {
    "dim_learner": 4,
    "dim_topic": 10,
    "dim_content_type": 6,
    "dim_reference_source": 6,
    "fact_learning_interaction": 6,
    "fact_practice_attempt": 5,
    "fact_learning_session": 3,
    "fact_learning_feedback": 11,
    "fact_ai_insight_validation": 9,
    "fact_learner_concept_state": 12,
    "agg_learner_overview_daily": 9,
    "agg_concept_weakness_daily": 17,
    "agg_learning_progress_daily": 9,
    "agg_illusion_of_learning": 3,
    "ml_learning_difficulty_features": 3,
}

GRAIN_CHECKS: Dict[str, Sequence[str]] = {
    "dim_learner": ["user_id", "profile_updated_at"],
    "dim_topic": ["taxonomy_id"],
    "dim_content_type": ["content_type_id"],
    "dim_reference_source": ["reference_id"],
    "fact_learning_interaction": ["interaction_key"],
    "fact_practice_attempt": ["attempt_key"],
    "fact_learning_session": ["session_id", "user_key"],
    "fact_learning_feedback": ["feedback_key"],
    "fact_ai_insight_validation": ["validation_key"],
    "fact_learner_concept_state": ["user_key", "topic_key", "state_version"],
    "agg_learner_overview_daily": ["user_key", "date"],
    "agg_concept_weakness_daily": ["user_key", "topic_key", "date"],
    "agg_learning_progress_daily": ["user_key", "date"],
    "agg_illusion_of_learning": ["user_key", "topic_key", "session_id"],
    "ml_learning_difficulty_features": ["user_key", "topic_key", "session_id"],
}

REQUIRED_KEY_CHECKS: Dict[str, Sequence[str]] = {
    "dim_learner": ["user_key", "user_id", "profile_updated_at", "valid_from", "is_current"],
    "dim_topic": ["topic_key", "topic_id"],
    "dim_content_type": ["content_type_key", "content_type_id"],
    "dim_reference_source": ["reference_key", "reference_id"],
    "fact_learning_interaction": ["interaction_key", "event_id", "user_key", "session_id"],
    "fact_practice_attempt": ["attempt_key", "attempt_id", "event_id", "user_key", "topic_key", "session_id"],
    "fact_learning_session": ["session_id", "user_key", "session_start_time"],
    "fact_learning_feedback": ["feedback_key", "user_key", "feedback_stage", "feedback_time"],
    "fact_ai_insight_validation": ["validation_key", "validation_id", "insight_id", "event_id", "user_key", "topic_key", "reference_key"],
    "fact_learner_concept_state": ["concept_state_key", "user_key", "topic_key", "state_version", "last_evidence_time"],
    "agg_learner_overview_daily": ["user_key", "date"],
    "agg_concept_weakness_daily": ["user_key", "topic_key", "date"],
    "agg_learning_progress_daily": ["user_key", "date"],
    "agg_illusion_of_learning": ["user_key", "topic_key", "session_id"],
    "ml_learning_difficulty_features": ["user_key", "topic_key", "session_id"],
}

USER_KEY_TABLES = [
    "fact_learning_interaction",
    "fact_practice_attempt",
    "fact_learning_session",
    "fact_learning_feedback",
    "fact_ai_insight_validation",
    "fact_learner_concept_state",
    "agg_learner_overview_daily",
    "agg_concept_weakness_daily",
    "agg_learning_progress_daily",
    "agg_illusion_of_learning",
    "ml_learning_difficulty_features",
]

TOPIC_KEY_TABLES = [
    "fact_practice_attempt",
    "fact_ai_insight_validation",
    "fact_learner_concept_state",
    "agg_concept_weakness_daily",
    "agg_illusion_of_learning",
    "ml_learning_difficulty_features",
]


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("gold_quality_job").getOrCreate()


def full_table(table_name: str) -> str:
    return f"{GOLD_PREFIX}{table_name}"


def status_for(failed_rows: int) -> str:
    return STATUS_PASS if failed_rows == 0 else STATUS_FAIL


def add_result(
    results: List[RuleResult],
    rule_group: str,
    table_name: str,
    rule_name: str,
    total_rows: int,
    failed_rows: int,
) -> None:
    result = RuleResult(
        rule_group=rule_group,
        table_name=table_name,
        rule_name=rule_name,
        total_rows=int(total_rows),
        failed_rows=int(failed_rows),
        status=status_for(int(failed_rows)),
    )
    logger.info(
        "Gold quality rule | group=%s table=%s rule=%s total_rows=%s failed_rows=%s status=%s",
        result.rule_group,
        result.table_name,
        result.rule_name,
        result.total_rows,
        result.failed_rows,
        result.status,
    )
    results += [result]


def any_null(columns: Sequence[str]) -> F.Column:
    condition = None
    for column in columns:
        current = F.col(column).isNull()
        condition = current if condition is None else condition | current
    return condition


def outside_zero_one(column_name: str) -> F.Column:
    return F.col(column_name).isNotNull() & ((F.col(column_name) < 0) | (F.col(column_name) > 1))


def run_count_checks(spark: SparkSession, results: List[RuleResult]) -> None:
    for table_name, minimum_count in MINIMUM_COUNTS.items():
        actual_count = spark.table(full_table(table_name)).count()
        failed_rows = 0 if actual_count >= minimum_count else minimum_count - actual_count
        logger.info("Minimum row count | table=%s minimum=%s actual=%s", table_name, minimum_count, actual_count)
        add_result(results, "row_count", table_name, f"minimum_rows_{minimum_count}", actual_count, failed_rows)


def run_grain_checks(spark: SparkSession, results: List[RuleResult]) -> None:
    for table_name, grain_columns in GRAIN_CHECKS.items():
        df = spark.table(full_table(table_name))
        total_rows = df.count()
        distinct_rows = df.select(*grain_columns).distinct().count()
        add_result(results, "grain", table_name, "duplicate_grain_rows", total_rows, total_rows - distinct_rows)


def run_required_key_checks(spark: SparkSession, results: List[RuleResult]) -> None:
    for table_name, required_columns in REQUIRED_KEY_CHECKS.items():
        df = spark.table(full_table(table_name))
        total_rows = df.count()
        failed_rows = df.filter(any_null(required_columns)).count()
        add_result(results, "required_keys", table_name, "missing_required_keys", total_rows, failed_rows)


def run_foreign_key_checks(spark: SparkSession, results: List[RuleResult]) -> None:
    dim_learner_df = spark.table(full_table("dim_learner")).select("user_key").distinct()
    dim_topic_df = spark.table(full_table("dim_topic")).select("topic_key").distinct()
    dim_reference_df = spark.table(full_table("dim_reference_source")).select("reference_key").distinct()

    for table_name in USER_KEY_TABLES:
        source_keys_df = spark.table(full_table(table_name)).select("user_key").filter(F.col("user_key").isNotNull()).distinct()
        failed_rows = source_keys_df.join(dim_learner_df, ["user_key"], "left_anti").count()
        add_result(results, "foreign_keys", table_name, "user_key_to_dim_learner", source_keys_df.count(), failed_rows)

    for table_name in TOPIC_KEY_TABLES:
        source_keys_df = spark.table(full_table(table_name)).select("topic_key").filter(F.col("topic_key").isNotNull()).distinct()
        failed_rows = source_keys_df.join(dim_topic_df, ["topic_key"], "left_anti").count()
        add_result(results, "foreign_keys", table_name, "topic_key_to_dim_topic", source_keys_df.count(), failed_rows)

    reference_keys_df = (
        spark.table(full_table("fact_ai_insight_validation"))
        .select("reference_key")
        .filter(F.col("reference_key").isNotNull())
        .distinct()
    )
    failed_rows = reference_keys_df.join(dim_reference_df, ["reference_key"], "left_anti").count()
    add_result(results, "foreign_keys", "fact_ai_insight_validation", "reference_key_to_dim_reference_source", reference_keys_df.count(), failed_rows)


def run_range_checks(spark: SparkSession, results: List[RuleResult]) -> None:
    fact_attempt_df = spark.table(full_table("fact_practice_attempt"))
    attempt_invalid = fact_attempt_df.filter(
        (F.col("score") < 0)
        | (F.col("score") > 1)
        | (F.col("hints_used") < 0)
        | (F.col("attempt_duration_seconds") < 0)
        | (F.col("attempt_number") < 1)
    ).count()
    add_result(results, "ranges", "fact_practice_attempt", "practice_attempt_ranges", fact_attempt_df.count(), attempt_invalid)

    validation_df = spark.table(full_table("fact_ai_insight_validation"))
    validation_invalid = validation_df.filter(
        outside_zero_one("extraction_confidence")
        | outside_zero_one("semantic_match_score")
        | outside_zero_one("reliability_score")
    ).count()
    add_result(results, "ranges", "fact_ai_insight_validation", "ai_validation_score_ranges", validation_df.count(), validation_invalid)

    state_df = spark.table(full_table("fact_learner_concept_state"))
    state_invalid = state_df.filter(
        outside_zero_one("mastery_score")
        | outside_zero_one("difficulty_score")
        | outside_zero_one("confidence_score")
        | outside_zero_one("struggle_risk_score")
        | (F.col("repeated_mistake_count") < 0)
        | (F.col("evidence_count") < 1)
    ).count()
    add_result(results, "ranges", "fact_learner_concept_state", "concept_state_ranges", state_df.count(), state_invalid)

    illusion_df = spark.table(full_table("agg_illusion_of_learning"))
    illusion_invalid = illusion_df.filter(
        (F.col("practice_score") < 0)
        | (F.col("practice_score") > 1)
        | (F.col("illusion_gap_score") < -1)
        | (F.col("illusion_gap_score") > 1)
    ).count()
    add_result(results, "ranges", "agg_illusion_of_learning", "illusion_score_ranges", illusion_df.count(), illusion_invalid)

    weakness_df = spark.table(full_table("agg_concept_weakness_daily"))
    weakness_invalid = weakness_df.filter(
        outside_zero_one("failure_rate")
        | outside_zero_one("avg_score")
        | outside_zero_one("avg_confidence_score")
        | outside_zero_one("difficulty_score")
    ).count()
    add_result(results, "ranges", "agg_concept_weakness_daily", "weakness_score_ranges", weakness_df.count(), weakness_invalid)

    progress_df = spark.table(full_table("agg_learning_progress_daily"))
    progress_invalid = progress_df.filter(
        outside_zero_one("avg_mastery_score")
        | outside_zero_one("avg_practice_score")
        | outside_zero_one("hint_usage_rate")
        | outside_zero_one("still_confused_rate")
    ).count()
    add_result(results, "ranges", "agg_learning_progress_daily", "progress_score_ranges", progress_df.count(), progress_invalid)


def run_scd2_checks(spark: SparkSession, results: List[RuleResult]) -> None:
    dim_learner_df = spark.table(full_table("dim_learner"))
    total_rows = dim_learner_df.count()

    current_count_failures = (
        dim_learner_df
        .groupBy("user_id")
        .agg(F.sum(F.when(F.col("is_current") == True, 1).otherwise(0)).alias("current_rows"))
        .filter(F.col("current_rows") != 1)
        .count()
    )
    add_result(results, "scd2", "dim_learner", "one_current_row_per_user", total_rows, current_count_failures)

    historical_open_rows = dim_learner_df.filter((F.col("is_current") == False) & F.col("valid_to").isNull()).count()
    add_result(results, "scd2", "dim_learner", "historical_rows_have_valid_to", total_rows, historical_open_rows)

    current_closed_rows = dim_learner_df.filter((F.col("is_current") == True) & F.col("valid_to").isNotNull()).count()
    add_result(results, "scd2", "dim_learner", "current_rows_have_null_valid_to", total_rows, current_closed_rows)

    missing_valid_from = dim_learner_df.filter(F.col("valid_from").isNull()).count()
    add_result(results, "scd2", "dim_learner", "valid_from_not_null", total_rows, missing_valid_from)

    invalid_periods = dim_learner_df.filter(F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))).count()
    add_result(results, "scd2", "dim_learner", "valid_to_greater_than_valid_from", total_rows, invalid_periods)

    version_window = Window.partitionBy("user_id").orderBy("valid_from")
    overlapped_rows = (
        dim_learner_df
        .withColumn("next_valid_from", F.lead("valid_from").over(version_window))
        .filter(
            F.col("valid_to").isNotNull()
            & F.col("next_valid_from").isNotNull()
            & (F.col("valid_to") > F.col("next_valid_from"))
        )
        .count()
    )
    add_result(results, "scd2", "dim_learner", "no_overlapping_validity_periods", total_rows, overlapped_rows)


def run_job(spark: SparkSession) -> int:
    logger.info("gold_quality_job started.")
    results: List[RuleResult] = []
    run_count_checks(spark, results)
    run_grain_checks(spark, results)
    run_required_key_checks(spark, results)
    run_foreign_key_checks(spark, results)
    run_range_checks(spark, results)
    run_scd2_checks(spark, results)

    total_checks = len(results)
    failed_checks = sum(1 for result in results if result.status == STATUS_FAIL)
    passed_checks = total_checks - failed_checks
    logger.info(
        "Gold quality summary | total_checks=%s passed_checks=%s failed_checks=%s",
        total_checks,
        passed_checks,
        failed_checks,
    )
    if failed_checks > 0:
        logger.error("FINAL GOLD QUALITY STATUS: FAIL")
        return 1

    logger.info("FINAL GOLD QUALITY STATUS: PASS")
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("gold_quality_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
