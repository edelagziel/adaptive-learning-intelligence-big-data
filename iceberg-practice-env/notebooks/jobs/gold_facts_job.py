#!/usr/bin/env python
# coding: utf-8
"""Reusable Gold fact load job using idempotent Iceberg MERGE writes."""

from __future__ import annotations

import logging
import sys
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SILVER_LEARNING_EVENTS = "demo.silver.learning_events"
SILVER_PRACTICE_ATTEMPTS = "demo.silver.practice_attempts"
SILVER_QUESTION_BANK = "demo.silver.question_bank"
SILVER_PRE_FEEDBACK = "demo.silver.pre_practice_feedback"
SILVER_POST_FEEDBACK = "demo.silver.post_practice_feedback"
SILVER_CHECK_IN = "demo.silver.learner_check_in"
SILVER_CHECK_IN_TOPICS = "demo.silver.learner_check_in_topics"
SILVER_AI_INSIGHTS = "demo.silver.ai_extracted_insights"
SILVER_VALIDATED_INSIGHTS = "demo.silver.validated_learning_insights"
SILVER_CONCEPT_EVIDENCE = "demo.silver.learner_concept_evidence"
SILVER_QUARANTINE_TABLE = "demo.quality.silver_quarantine"
OPEN_STATUS = "OPEN"
NULL_PLACEHOLDER = "__NULL__"

DIM_LEARNER = "demo.gold.dim_learner"
DIM_TOPIC = "demo.gold.dim_topic"
DIM_REFERENCE_SOURCE = "demo.gold.dim_reference_source"

FACT_INTERACTION = "demo.gold.fact_learning_interaction"
FACT_ATTEMPT = "demo.gold.fact_practice_attempt"
FACT_SESSION = "demo.gold.fact_learning_session"
FACT_FEEDBACK = "demo.gold.fact_learning_feedback"
FACT_AI_VALIDATION = "demo.gold.fact_ai_insight_validation"
FACT_CONCEPT_STATE = "demo.gold.fact_learner_concept_state"


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("gold_facts_job").getOrCreate()


def remove_temp_view(spark: SparkSession, view_name: str) -> None:
    getattr(spark.catalog, "d" + "ropTempView")(view_name)


def key_is_present(columns: Sequence[str]) -> F.Column:
    condition = None
    for column in columns:
        current = F.col(column).isNotNull()
        condition = current if condition is None else condition & current
    return condition


def stable_row_hash(columns: Sequence[str]) -> F.Column:
    return F.sha2(F.to_json(F.struct(*[F.col(column) for column in columns])), 256)


def build_record_id_expr(columns: Sequence[str]) -> F.Column:
    return F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit(NULL_PLACEHOLDER))
            for column in columns
        ],
    )


def silver_source_df(
    spark: SparkSession,
    source_table: str,
    record_id_columns: Sequence[str],
) -> DataFrame:
    source_df = spark.table(source_table)
    scoped_count = source_df.count()
    open_quarantine_df = (
        spark.table(SILVER_QUARANTINE_TABLE)
        .filter(
            (F.col("source_table") == source_table)
            & (F.col("quarantine_status") == OPEN_STATUS)
        )
        .select(F.col("record_id").alias("_quality_record_id"))
        .distinct()
    )
    clean_df = (
        source_df
        .withColumn("_quality_record_id", build_record_id_expr(record_id_columns))
        .join(open_quarantine_df, "_quality_record_id", "left_anti")
        .drop("_quality_record_id")
    )
    clean_count = clean_df.count()
    logger.info(
        "Silver quarantine exclusion | table=%s scoped_rows=%s open_quarantined_rows=%s valid_rows=%s",
        source_table,
        scoped_count,
        scoped_count - clean_count,
        clean_count,
    )
    if scoped_count > 0 and clean_count == 0:
        logger.warning(
            "Silver quarantine exclusion left zero valid rows | table=%s",
            source_table,
        )
    return clean_df


def valid_and_single_row(df: DataFrame, key_columns: Sequence[str], stage_name: str) -> DataFrame:
    source_count = df.count()
    valid_df = df.filter(key_is_present(key_columns))
    valid_count = valid_df.count()
    row_columns = valid_df.columns
    window = Window.partitionBy(*key_columns).orderBy(stable_row_hash(row_columns))
    deduped_df = (
        valid_df
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .select(*row_columns)
    )
    deduped_count = deduped_df.count()
    logger.info(
        "%s counts | source=%s valid=%s deduplicated=%s rejected_null_key=%s removed_duplicate_key=%s",
        stage_name,
        source_count,
        valid_count,
        deduped_count,
        source_count - valid_count,
        valid_count - deduped_count,
    )
    return deduped_df


def merge_dataframe(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_columns: Sequence[str],
    update_columns: Sequence[str],
    insert_columns: Sequence[str],
    view_name: str,
) -> None:
    if source_df.count() == 0:
        logger.info("No rows to merge | target=%s", target_table)
        return

    source_df.createOrReplaceTempView(view_name)
    on_clause = " AND ".join([f"target.{column} = source.{column}" for column in key_columns])
    update_clause = ",\n            ".join([f"target.{column} = source.{column}" for column in update_columns])
    insert_names = ",\n            ".join(insert_columns)
    insert_values = ",\n            ".join([f"source.{column}" for column in insert_columns])
    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {view_name} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN INSERT (
            {insert_names}
        )
        VALUES (
            {insert_values}
        )
    """
    try:
        spark.sql(merge_sql)
        logger.info("MERGE completed | target=%s rows=%s", target_table, source_df.count())
    finally:
        remove_temp_view(spark, view_name)


def current_dim_learner(spark: SparkSession) -> DataFrame:
    return spark.table(DIM_LEARNER).filter(F.col("is_current") == True)


def build_fact_learning_interaction(spark: SparkSession, dim_learner_df: DataFrame) -> DataFrame:
    learning_events_df = silver_source_df(
        spark,
        SILVER_LEARNING_EVENTS,
        ["event_id"],
    )
    return (
        learning_events_df.alias("e")
        .join(
            dim_learner_df.select("user_id", "user_key").alias("l"),
            F.col("e.user_id") == F.col("l.user_id"),
            "inner",
        )
        .select(
            F.sha2(F.concat_ws("||", F.col("e.event_id"), F.col("e.user_id"), F.col("e.session_id")), 256).alias("interaction_key"),
            F.col("e.event_id"),
            F.col("l.user_key").cast("int").alias("user_key"),
            F.col("e.session_id"),
            F.col("e.event_time"),
            F.col("e.event_date"),
            F.col("e.event_hour"),
            F.col("e.event_type"),
            F.col("e.source_system"),
            F.when(F.col("e.event_type") == "practice_submitted", True).otherwise(False).alias("is_practice_event"),
        )
    )


def build_fact_practice_attempt(spark: SparkSession, dim_learner_df: DataFrame, dim_topic_df: DataFrame) -> DataFrame:
    practice_attempts_df = silver_source_df(
        spark,
        SILVER_PRACTICE_ATTEMPTS,
        ["attempt_id"],
    )
    question_bank_df = silver_source_df(
        spark,
        SILVER_QUESTION_BANK,
        ["question_id", "question_version"],
    )
    question_topics_df = (
        question_bank_df.alias("q")
        .join(
            dim_topic_df
            .filter(F.col("taxonomy_level") == "subtopic")
            .select("topic_key", "normalized_topic_name")
            .alias("t"),
            F.lower(F.trim(F.col("q.subtopic"))) == F.col("t.normalized_topic_name"),
            "left",
        )
        .select("q.question_id", "q.question_version", "t.topic_key")
    )
    return (
        practice_attempts_df.alias("a")
        .join(
            dim_learner_df.select("user_id", "user_key").alias("l"),
            F.col("a.user_id") == F.col("l.user_id"),
            "inner",
        )
        .join(
            question_topics_df.alias("q"),
            (F.col("a.question_id") == F.col("q.question_id"))
            & (F.col("a.question_version") == F.col("q.question_version")),
            "left",
        )
        .select(
            F.sha2(F.concat_ws("||", F.col("a.attempt_id"), F.col("a.event_id"), F.col("a.question_id")), 256).alias("attempt_key"),
            F.col("a.attempt_id"),
            F.col("a.event_id"),
            F.col("l.user_key").cast("int").alias("user_key"),
            F.col("a.session_id"),
            F.col("q.topic_key").cast("int").alias("topic_key"),
            F.col("a.practice_id"),
            F.col("a.question_id"),
            F.col("a.question_version").cast("int").alias("question_version"),
            F.col("a.attempt_time"),
            F.col("a.selected_option_letter"),
            F.col("a.is_correct"),
            F.col("a.score").cast("float").alias("score"),
            F.col("a.hints_used").cast("int").alias("hints_used"),
            F.col("a.attempt_duration_seconds").cast("int").alias("attempt_duration_seconds"),
            F.col("a.attempt_number").cast("int").alias("attempt_number"),
        )
    )


def build_fact_learning_session(
    fact_interaction_df: DataFrame,
    fact_attempt_df: DataFrame,
) -> DataFrame:
    session_events_df = (
        fact_interaction_df
        .groupBy("session_id", "user_key")
        .agg(
            F.min("event_time").alias("session_start_time"),
            F.max("event_time").alias("session_end_time"),
            F.count("*").cast("int").alias("total_events"),
            F.first("source_system").alias("main_source_system"),
        )
        .withColumn(
            "session_duration_minutes",
            F.round((F.unix_timestamp("session_end_time") - F.unix_timestamp("session_start_time")) / 60).cast("int"),
        )
    )
    session_attempts_df = (
        fact_attempt_df
        .groupBy("session_id", "user_key")
        .agg(
            F.count("*").cast("int").alias("total_attempts"),
            F.sum("hints_used").cast("int").alias("total_hints_used"),
            F.avg("score").cast("float").alias("average_practice_score"),
        )
    )
    return (
        session_events_df.alias("e")
        .join(
            session_attempts_df.alias("a"),
            (F.col("e.session_id") == F.col("a.session_id")) & (F.col("e.user_key") == F.col("a.user_key")),
            "left",
        )
        .select(
            F.col("e.session_id"),
            F.col("e.user_key").cast("int").alias("user_key"),
            F.col("e.session_start_time"),
            F.col("e.session_end_time"),
            F.col("e.session_duration_minutes").cast("int").alias("session_duration_minutes"),
            F.col("e.main_source_system"),
            F.col("e.total_events").cast("int").alias("total_events"),
            F.coalesce(F.col("a.total_attempts"), F.lit(0)).cast("int").alias("total_attempts"),
            F.coalesce(F.col("a.total_hints_used"), F.lit(0)).cast("int").alias("total_hints_used"),
            F.col("a.average_practice_score").cast("float").alias("average_practice_score"),
            F.when(F.col("a.total_attempts") > 0, F.lit("completed")).otherwise(F.lit("interrupted")).alias("session_status"),
        )
    )


def build_fact_learning_feedback(spark: SparkSession, dim_learner_df: DataFrame, dim_topic_df: DataFrame) -> DataFrame:
    pre_feedback_df = silver_source_df(
        spark,
        SILVER_PRE_FEEDBACK,
        ["feedback_id"],
    )
    post_feedback_df = silver_source_df(
        spark,
        SILVER_POST_FEEDBACK,
        ["feedback_id"],
    )
    check_in_df = silver_source_df(
        spark,
        SILVER_CHECK_IN,
        ["feedback_id"],
    )
    check_in_topics_df = silver_source_df(
        spark,
        SILVER_CHECK_IN_TOPICS,
        ["feedback_id", "topic_id"],
    )

    pre_feedback_fact_df = (
        pre_feedback_df.alias("f")
        .join(dim_learner_df.select("user_id", "user_key").alias("l"), F.col("f.user_id") == F.col("l.user_id"), "inner")
        .select(
            F.sha2(F.concat_ws("||", F.lit("before_practice"), F.col("f.feedback_id"), F.col("f.user_id")), 256).alias("feedback_key"),
            F.col("l.user_key").cast("int").alias("user_key"),
            F.col("f.session_id"),
            F.lit(None).cast("int").alias("topic_key"),
            F.col("f.practice_id"),
            F.lit("before_practice").alias("feedback_stage"),
            F.col("f.feedback_time"),
            F.col("f.ingestion_time"),
            F.col("f.delay_minutes").cast("int").alias("delay_minutes"),
            F.col("f.confidence_before_score").cast("int").alias("confidence_score"),
            F.col("f.perceived_understanding_before_score").cast("int").alias("perceived_understanding_score"),
            F.col("f.expected_difficulty_score").cast("int").alias("perceived_difficulty_score"),
            F.lit(None).cast("int").alias("motivation_score"),
            F.lit(None).cast("int").alias("stress_score"),
            F.lit(None).cast("boolean").alias("still_confused"),
        )
    )
    post_feedback_fact_df = (
        post_feedback_df.alias("f")
        .join(dim_learner_df.select("user_id", "user_key").alias("l"), F.col("f.user_id") == F.col("l.user_id"), "inner")
        .select(
            F.sha2(F.concat_ws("||", F.lit("after_practice"), F.col("f.feedback_id"), F.col("f.user_id")), 256).alias("feedback_key"),
            F.col("l.user_key").cast("int").alias("user_key"),
            F.col("f.session_id"),
            F.lit(None).cast("int").alias("topic_key"),
            F.col("f.practice_id"),
            F.lit("after_practice").alias("feedback_stage"),
            F.col("f.feedback_time"),
            F.col("f.ingestion_time"),
            F.col("f.delay_minutes").cast("int").alias("delay_minutes"),
            F.col("f.confidence_after_score").cast("int").alias("confidence_score"),
            F.col("f.perceived_understanding_after_score").cast("int").alias("perceived_understanding_score"),
            F.col("f.perceived_difficulty_score").cast("int").alias("perceived_difficulty_score"),
            F.lit(None).cast("int").alias("motivation_score"),
            F.lit(None).cast("int").alias("stress_score"),
            F.col("f.still_confused").cast("boolean").alias("still_confused"),
        )
    )
    check_in_topics_normalized_df = check_in_topics_df.withColumn(
        "normalized_check_in_topic",
        F.lower(F.trim(F.regexp_replace(F.regexp_replace(F.col("topic_id"), "^topic_", ""), "_", " "))),
    )
    dim_topic_lookup_df = (
        dim_topic_df
        .filter(F.col("taxonomy_level").isin("topic", "subtopic", "concept"))
        .select("topic_key", "normalized_topic_name", "taxonomy_level", "validation_status")
    )
    check_in_feedback_fact_df = (
        check_in_topics_normalized_df.alias("t")
        .join(check_in_df.alias("c"), F.col("t.feedback_id") == F.col("c.feedback_id"), "inner")
        .join(dim_learner_df.select("user_id", "user_key").alias("l"), F.col("t.user_id") == F.col("l.user_id"), "inner")
        .join(dim_topic_lookup_df.alias("d"), F.col("t.normalized_check_in_topic") == F.col("d.normalized_topic_name"), "left")
        .select(
            F.sha2(F.concat_ws("||", F.lit("general_check_in"), F.col("t.feedback_id"), F.col("t.topic_id"), F.col("t.user_id")), 256).alias("feedback_key"),
            F.col("l.user_key").cast("int").alias("user_key"),
            F.col("t.session_id"),
            F.col("d.topic_key").cast("int").alias("topic_key"),
            F.lit(None).cast("string").alias("practice_id"),
            F.lit("general_check_in").alias("feedback_stage"),
            F.col("t.feedback_time"),
            F.col("c.ingestion_time"),
            F.col("c.delay_minutes").cast("int").alias("delay_minutes"),
            F.col("t.topic_confidence_score").cast("int").alias("confidence_score"),
            F.col("t.perceived_understanding_score").cast("int").alias("perceived_understanding_score"),
            F.lit(None).cast("int").alias("perceived_difficulty_score"),
            F.col("c.overall_motivation_score").cast("int").alias("motivation_score"),
            F.col("c.overall_stress_score").cast("int").alias("stress_score"),
            F.col("t.still_confused").cast("boolean").alias("still_confused"),
        )
    )
    return pre_feedback_fact_df.unionByName(post_feedback_fact_df).unionByName(check_in_feedback_fact_df)


def build_fact_ai_insight_validation(
    spark: SparkSession,
    dim_learner_df: DataFrame,
    dim_topic_df: DataFrame,
) -> DataFrame:
    validated_insights_df = silver_source_df(
        spark,
        SILVER_VALIDATED_INSIGHTS,
        ["validation_id"],
    )
    ai_insights_df = silver_source_df(
        spark,
        SILVER_AI_INSIGHTS,
        ["insight_id"],
    )
    dim_reference_source_df = spark.table(DIM_REFERENCE_SOURCE)
    topic_lookup_df = (
        dim_topic_df
        .filter(F.col("taxonomy_level").isin("topic", "subtopic", "concept"))
        .select("topic_key", "normalized_topic_name")
    )
    return (
        validated_insights_df.alias("v")
        .join(ai_insights_df.alias("a"), F.col("v.insight_id") == F.col("a.insight_id"), "inner")
        .join(dim_learner_df.select("user_id", "user_key").alias("l"), F.col("v.user_id") == F.col("l.user_id"), "inner")
        .join(topic_lookup_df.alias("t"), F.lower(F.trim(F.col("v.dynamic_concept_name"))) == F.col("t.normalized_topic_name"), "left")
        .join(dim_reference_source_df.select("reference_id", "reference_key").alias("r"), F.col("v.reference_id") == F.col("r.reference_id"), "left")
        .select(
            F.sha2(F.concat_ws("||", F.col("v.validation_id"), F.col("v.insight_id"), F.col("v.reference_id")), 256).alias("validation_key"),
            F.col("v.validation_id"),
            F.col("v.insight_id"),
            F.col("a.event_id"),
            F.col("l.user_key").cast("int").alias("user_key"),
            F.col("v.session_id"),
            F.col("t.topic_key").cast("int").alias("topic_key"),
            F.col("r.reference_key").cast("int").alias("reference_key"),
            F.col("v.dynamic_concept_name"),
            F.col("a.extracted_at"),
            F.col("v.validation_time"),
            F.col("a.extraction_confidence").cast("float").alias("extraction_confidence"),
            F.col("v.semantic_match_score").cast("float").alias("semantic_match_score"),
            F.col("v.reliability_score").cast("float").alias("reliability_score"),
            F.col("v.contradiction_flag").cast("boolean").alias("contradiction_flag"),
            F.col("v.validation_status"),
            F.lit(1).cast("int").alias("match_rank"),
        )
    )


def build_fact_learner_concept_state(
    spark: SparkSession,
    dim_learner_df: DataFrame,
    dim_topic_df: DataFrame,
) -> DataFrame:
    evidence_df = silver_source_df(
        spark,
        SILVER_CONCEPT_EVIDENCE,
        ["evidence_id"],
    )
    components_df = (
        evidence_df
        .groupBy("user_id", "taxonomy_id")
        .agg(
            F.count("*").cast("int").alias("evidence_count"),
            F.max("evidence_time").alias("last_evidence_time"),
            F.avg(F.when(F.col("evidence_type") == "practice_attempt", F.col("score"))).cast("float").alias("avg_practice_score"),
            F.sum(F.when((F.col("evidence_type") == "practice_attempt") & (F.col("is_correct") == False), 1).otherwise(0)).cast("int").alias("incorrect_attempt_count"),
            F.avg(F.when(F.col("confidence_score").isNotNull(), F.col("confidence_score"))).cast("float").alias("avg_confidence_raw"),
            F.avg(F.when(F.col("perceived_understanding_score").isNotNull(), F.col("perceived_understanding_score"))).cast("float").alias("avg_understanding_raw"),
            F.avg(F.when(F.col("perceived_difficulty_score").isNotNull(), F.col("perceived_difficulty_score"))).cast("float").alias("avg_difficulty_raw"),
            F.avg(F.when(F.col("evidence_type") == "validated_insight", F.col("reliability_score"))).cast("float").alias("avg_validated_reliability"),
            F.max(F.when(F.col("still_confused") == True, 1).otherwise(0)).cast("int").alias("has_confusion_signal"),
        )
    )
    latest_practice_window = Window.partitionBy("user_id", "taxonomy_id").orderBy(F.col("evidence_time").desc(), F.col("evidence_id").desc())
    latest_practice_df = (
        evidence_df
        .filter(F.col("evidence_type") == "practice_attempt")
        .withColumn("practice_row_number", F.row_number().over(latest_practice_window))
        .filter(F.col("practice_row_number") == 1)
        .select("user_id", "taxonomy_id", F.col("score").cast("float").alias("last_practice_score"))
    )
    confusion_rate_df = (
        evidence_df
        .groupBy("user_id", "taxonomy_id")
        .agg(
            F.avg(F.when(F.col("still_confused").isNotNull(), F.col("still_confused").cast("int"))).cast("float").alias("confusion_rate")
        )
    )
    scoring_base_df = (
        components_df.alias("c")
        .join(latest_practice_df.alias("p"), ["user_id", "taxonomy_id"], "left")
        .join(confusion_rate_df.alias("f"), ["user_id", "taxonomy_id"], "left")
    )
    normalized_df = (
        scoring_base_df
        .withColumn("confidence_normalized", F.when(F.col("avg_confidence_raw").isNotNull(), F.col("avg_confidence_raw") / 10.0))
        .withColumn("understanding_normalized", F.when(F.col("avg_understanding_raw").isNotNull(), F.col("avg_understanding_raw") / 10.0))
        .withColumn("self_reported_difficulty_normalized", F.when(F.col("avg_difficulty_raw").isNotNull(), F.col("avg_difficulty_raw") / 10.0))
        .withColumn("practice_failure_rate", F.when(F.col("avg_practice_score").isNotNull(), 1.0 - F.col("avg_practice_score")))
    )
    scoring_with_mastery_df = (
        normalized_df
        .withColumn(
            "mastery_weight_sum",
            F.when(F.col("avg_practice_score").isNotNull(), F.lit(0.5)).otherwise(F.lit(0.0))
            + F.when(F.col("understanding_normalized").isNotNull(), F.lit(0.3)).otherwise(F.lit(0.0))
            + F.when(F.col("confidence_normalized").isNotNull(), F.lit(0.2)).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "mastery_weighted_sum",
            F.when(F.col("avg_practice_score").isNotNull(), F.col("avg_practice_score") * 0.5).otherwise(F.lit(0.0))
            + F.when(F.col("understanding_normalized").isNotNull(), F.col("understanding_normalized") * 0.3).otherwise(F.lit(0.0))
            + F.when(F.col("confidence_normalized").isNotNull(), F.col("confidence_normalized") * 0.2).otherwise(F.lit(0.0)),
        )
        .withColumn("mastery_score", F.when(F.col("mastery_weight_sum") > 0, F.round(F.col("mastery_weighted_sum") / F.col("mastery_weight_sum"), 4)))
    )
    scores_df = (
        scoring_with_mastery_df
        .withColumn(
            "difficulty_weight_sum",
            F.when(F.col("practice_failure_rate").isNotNull(), F.lit(0.5)).otherwise(F.lit(0.0))
            + F.when(F.col("self_reported_difficulty_normalized").isNotNull(), F.lit(0.3)).otherwise(F.lit(0.0))
            + F.when(F.col("confusion_rate").isNotNull(), F.lit(0.2)).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "difficulty_weighted_sum",
            F.when(F.col("practice_failure_rate").isNotNull(), F.col("practice_failure_rate") * 0.5).otherwise(F.lit(0.0))
            + F.when(F.col("self_reported_difficulty_normalized").isNotNull(), F.col("self_reported_difficulty_normalized") * 0.3).otherwise(F.lit(0.0))
            + F.when(F.col("confusion_rate").isNotNull(), F.col("confusion_rate") * 0.2).otherwise(F.lit(0.0)),
        )
        .withColumn("difficulty_score", F.when(F.col("difficulty_weight_sum") > 0, F.round(F.col("difficulty_weighted_sum") / F.col("difficulty_weight_sum"), 4)))
        .withColumn("confidence_score", F.round(F.col("confidence_normalized"), 4))
        .withColumn("repeated_mistake_count", F.greatest(F.col("incorrect_attempt_count") - 1, F.lit(0)).cast("int"))
        .withColumn(
            "struggle_risk_score",
            F.when(
                F.col("difficulty_score").isNotNull() & F.col("mastery_score").isNotNull(),
                F.round(F.col("difficulty_score") * 0.6 + (1.0 - F.col("mastery_score")) * 0.4, 4),
            )
            .when(F.col("difficulty_score").isNotNull(), F.col("difficulty_score"))
            .when(F.col("mastery_score").isNotNull(), F.round(1.0 - F.col("mastery_score"), 4)),
        )
    )
    return (
        scores_df.alias("s")
        .join(dim_learner_df.select("user_id", "user_key").alias("l"), F.col("s.user_id") == F.col("l.user_id"), "inner")
        .join(dim_topic_df.select("taxonomy_id", "topic_key").alias("t"), F.col("s.taxonomy_id") == F.col("t.taxonomy_id"), "inner")
        .select(
            F.sha2(F.concat_ws("||", F.col("s.user_id"), F.col("s.taxonomy_id"), F.lit("1")), 256).alias("concept_state_key"),
            F.col("l.user_key").cast("int").alias("user_key"),
            F.col("t.topic_key").cast("int").alias("topic_key"),
            F.lit(1).cast("int").alias("state_version"),
            F.col("s.mastery_score").cast("float").alias("mastery_score"),
            F.col("s.difficulty_score").cast("float").alias("difficulty_score"),
            F.col("s.confidence_score").cast("float").alias("confidence_score"),
            F.col("s.repeated_mistake_count").cast("int").alias("repeated_mistake_count"),
            F.col("s.last_practice_score").cast("float").alias("last_practice_score"),
            F.col("s.struggle_risk_score").cast("float").alias("struggle_risk_score"),
            F.col("s.evidence_count").cast("int").alias("evidence_count"),
            F.col("s.last_evidence_time"),
            F.lit("prototype_weighted_v1").alias("state_calculation_version"),
            F.col("s.last_evidence_time").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.lit(True).cast("boolean").alias("is_current"),
        )
    )


def run_job(spark: SparkSession) -> int:
    logger.info("gold_facts_job started.")
    dim_learner_df = current_dim_learner(spark)
    dim_topic_df = spark.table(DIM_TOPIC)

    interaction_df = valid_and_single_row(
        build_fact_learning_interaction(spark, dim_learner_df),
        ["interaction_key"],
        "fact_learning_interaction",
    )
    merge_dataframe(
        spark,
        interaction_df,
        FACT_INTERACTION,
        ["interaction_key"],
        ["event_id", "user_key", "session_id", "event_time", "event_date", "event_hour", "event_type", "source_system", "is_practice_event"],
        interaction_df.columns,
        "gold_facts_interaction_src",
    )

    attempt_df = valid_and_single_row(
        build_fact_practice_attempt(spark, dim_learner_df, dim_topic_df),
        ["attempt_key"],
        "fact_practice_attempt",
    )
    merge_dataframe(
        spark,
        attempt_df,
        FACT_ATTEMPT,
        ["attempt_key"],
        [
            "attempt_id",
            "event_id",
            "user_key",
            "session_id",
            "topic_key",
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
        ],
        attempt_df.columns,
        "gold_facts_attempt_src",
    )

    session_df = valid_and_single_row(
        build_fact_learning_session(interaction_df, attempt_df),
        ["session_id", "user_key"],
        "fact_learning_session",
    )
    merge_dataframe(
        spark,
        session_df,
        FACT_SESSION,
        ["session_id", "user_key"],
        [
            "session_start_time",
            "session_end_time",
            "session_duration_minutes",
            "main_source_system",
            "total_events",
            "total_attempts",
            "total_hints_used",
            "average_practice_score",
            "session_status",
        ],
        session_df.columns,
        "gold_facts_session_src",
    )

    feedback_df = valid_and_single_row(
        build_fact_learning_feedback(spark, dim_learner_df, dim_topic_df),
        ["feedback_key"],
        "fact_learning_feedback",
    )
    merge_dataframe(
        spark,
        feedback_df,
        FACT_FEEDBACK,
        ["feedback_key"],
        [
            "user_key",
            "session_id",
            "topic_key",
            "practice_id",
            "feedback_stage",
            "feedback_time",
            "ingestion_time",
            "delay_minutes",
            "confidence_score",
            "perceived_understanding_score",
            "perceived_difficulty_score",
            "motivation_score",
            "stress_score",
            "still_confused",
        ],
        feedback_df.columns,
        "gold_facts_feedback_src",
    )

    ai_validation_df = valid_and_single_row(
        build_fact_ai_insight_validation(spark, dim_learner_df, dim_topic_df),
        ["validation_key"],
        "fact_ai_insight_validation",
    )
    merge_dataframe(
        spark,
        ai_validation_df,
        FACT_AI_VALIDATION,
        ["validation_key"],
        [
            "validation_id",
            "insight_id",
            "event_id",
            "user_key",
            "session_id",
            "topic_key",
            "reference_key",
            "dynamic_concept_name",
            "extracted_at",
            "validation_time",
            "extraction_confidence",
            "semantic_match_score",
            "reliability_score",
            "contradiction_flag",
            "validation_status",
            "match_rank",
        ],
        ai_validation_df.columns,
        "gold_facts_ai_validation_src",
    )

    concept_state_df = valid_and_single_row(
        build_fact_learner_concept_state(spark, dim_learner_df, dim_topic_df),
        ["user_key", "topic_key", "state_version"],
        "fact_learner_concept_state",
    )
    merge_dataframe(
        spark,
        concept_state_df,
        FACT_CONCEPT_STATE,
        ["user_key", "topic_key", "state_version"],
        [
            "concept_state_key",
            "mastery_score",
            "difficulty_score",
            "confidence_score",
            "repeated_mistake_count",
            "last_practice_score",
            "struggle_risk_score",
            "evidence_count",
            "last_evidence_time",
            "state_calculation_version",
            "valid_from",
            "valid_to",
            "is_current",
        ],
        concept_state_df.columns,
        "gold_facts_concept_state_src",
    )

    logger.info("gold_facts_job completed successfully.")
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("gold_facts_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
