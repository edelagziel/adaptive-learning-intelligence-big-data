#!/usr/bin/env python
# coding: utf-8
"""Reusable Gold aggregation and ML feature load job."""

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

FACT_INTERACTION = "demo.gold.fact_learning_interaction"
FACT_ATTEMPT = "demo.gold.fact_practice_attempt"
FACT_SESSION = "demo.gold.fact_learning_session"
FACT_FEEDBACK = "demo.gold.fact_learning_feedback"
FACT_VALIDATION = "demo.gold.fact_ai_insight_validation"
FACT_CONCEPT_STATE = "demo.gold.fact_learner_concept_state"

AGG_OVERVIEW = "demo.gold.agg_learner_overview_daily"
AGG_WEAKNESS = "demo.gold.agg_concept_weakness_daily"
AGG_PROGRESS = "demo.gold.agg_learning_progress_daily"
AGG_ILLUSION = "demo.gold.agg_illusion_of_learning"
ML_FEATURES = "demo.gold.ml_learning_difficulty_features"


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("gold_aggregations_job").getOrCreate()


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
        "%s counts | source=%s valid=%s deduplicated=%s rejected_null_key=%s duplicate_key_rows=%s",
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
    row_count = source_df.count()
    if row_count == 0:
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
        logger.info("MERGE completed | target=%s rows=%s", target_table, row_count)
    finally:
        remove_temp_view(spark, view_name)


def build_learner_overview(
    fact_interaction_df: DataFrame,
    fact_attempt_df: DataFrame,
    fact_session_df: DataFrame,
    fact_feedback_df: DataFrame,
    fact_validation_df: DataFrame,
    fact_concept_state_df: DataFrame,
) -> DataFrame:
    learner_daily_spine_df = (
        fact_interaction_df.select("user_key", F.col("event_date").alias("date"))
        .unionByName(fact_session_df.select("user_key", F.to_date("session_start_time").alias("date")))
        .unionByName(fact_feedback_df.select("user_key", F.to_date("feedback_time").alias("date")))
        .unionByName(fact_validation_df.select("user_key", F.to_date("validation_time").alias("date")))
        .unionByName(fact_concept_state_df.select("user_key", F.to_date("valid_from").alias("date")))
        .filter(F.col("date").isNotNull())
        .distinct()
    )
    daily_sessions_df = (
        fact_session_df
        .groupBy("user_key", F.to_date("session_start_time").alias("date"))
        .agg(F.countDistinct("session_id").cast("int").alias("total_sessions"))
    )
    daily_interactions_df = (
        fact_interaction_df
        .groupBy("user_key", F.col("event_date").alias("date"))
        .agg(F.countDistinct("event_id").cast("int").alias("total_learning_events"))
    )
    daily_practice_topics_df = (
        fact_attempt_df
        .groupBy("user_key", F.to_date("attempt_time").alias("date"))
        .agg(F.countDistinct("topic_key").cast("int").alias("topics_practiced"))
    )
    daily_concept_state_df = (
        fact_concept_state_df
        .filter(F.col("is_current") == True)
        .groupBy("user_key", F.to_date("valid_from").alias("date"))
        .agg(
            F.sum(F.when(F.col("mastery_score").isNotNull() & (F.col("mastery_score") < 0.6), 1).otherwise(0)).cast("int").alias("weak_topics_count"),
            F.sum("repeated_mistake_count").cast("int").alias("repeated_mistakes_count"),
            F.avg("mastery_score").cast("float").alias("avg_mastery_score"),
            F.sum(F.when(F.col("struggle_risk_score").isNotNull() & (F.col("struggle_risk_score") >= 0.6), 1).otherwise(0)).cast("int").alias("at_risk_topics_count"),
        )
    )
    daily_validation_df = (
        fact_validation_df
        .groupBy("user_key", F.to_date("validation_time").alias("date"))
        .agg(F.avg("reliability_score").cast("float").alias("avg_reliability_score"))
    )
    daily_feedback_df = (
        fact_feedback_df
        .filter(F.col("feedback_stage") == "general_check_in")
        .groupBy("user_key", F.to_date("feedback_time").alias("date"))
        .agg(
            F.avg("motivation_score").cast("float").alias("avg_motivation_score"),
            F.avg("stress_score").cast("float").alias("avg_stress_score"),
            F.avg("perceived_understanding_score").cast("float").alias("avg_self_reported_understanding"),
        )
    )
    return (
        learner_daily_spine_df.alias("spine")
        .join(daily_sessions_df.alias("sessions"), ["user_key", "date"], "left")
        .join(daily_interactions_df.alias("interactions"), ["user_key", "date"], "left")
        .join(daily_practice_topics_df.alias("practice"), ["user_key", "date"], "left")
        .join(daily_concept_state_df.alias("state"), ["user_key", "date"], "left")
        .join(daily_validation_df.alias("validation"), ["user_key", "date"], "left")
        .join(daily_feedback_df.alias("feedback"), ["user_key", "date"], "left")
        .select(
            F.col("user_key").cast("int"),
            F.col("date"),
            F.coalesce(F.col("total_sessions"), F.lit(0)).cast("int").alias("total_sessions"),
            F.coalesce(F.col("total_learning_events"), F.lit(0)).cast("int").alias("total_learning_events"),
            F.coalesce(F.col("topics_practiced"), F.lit(0)).cast("int").alias("topics_practiced"),
            F.coalesce(F.col("weak_topics_count"), F.lit(0)).cast("int").alias("weak_topics_count"),
            F.coalesce(F.col("repeated_mistakes_count"), F.lit(0)).cast("int").alias("repeated_mistakes_count"),
            F.col("avg_mastery_score").cast("float"),
            F.coalesce(F.col("at_risk_topics_count"), F.lit(0)).cast("int").alias("at_risk_topics_count"),
            F.col("avg_reliability_score").cast("float"),
            F.col("avg_motivation_score").cast("float"),
            F.col("avg_stress_score").cast("float"),
            F.col("avg_self_reported_understanding").cast("float"),
        )
    )


def build_concept_weakness(
    fact_attempt_df: DataFrame,
    fact_feedback_df: DataFrame,
    fact_concept_state_df: DataFrame,
) -> DataFrame:
    daily_topic_practice_df = (
        fact_attempt_df
        .groupBy("user_key", "topic_key", F.to_date("attempt_time").alias("date"))
        .agg(
            F.count("*").cast("int").alias("total_attempts"),
            F.avg(F.when(F.col("is_correct") == False, 1.0).otherwise(0.0)).cast("float").alias("failure_rate"),
            F.avg("score").cast("float").alias("avg_score"),
            F.avg("hints_used").cast("float").alias("avg_hints_used"),
        )
    )
    daily_topic_state_df = (
        fact_concept_state_df
        .filter(F.col("is_current") == True)
        .select(
            F.col("user_key").cast("int"),
            F.col("topic_key").cast("int"),
            F.to_date("valid_from").alias("date"),
            F.col("repeated_mistake_count").cast("int"),
            F.col("confidence_score").cast("float").alias("state_confidence_score"),
            F.col("difficulty_score").cast("float"),
        )
    )
    daily_topic_feedback_df = (
        fact_feedback_df
        .filter(F.col("topic_key").isNotNull())
        .groupBy("user_key", "topic_key", F.to_date("feedback_time").alias("date"))
        .agg(F.avg("confidence_score").cast("float").alias("feedback_confidence_score"))
    )
    learner_topic_daily_spine_df = (
        daily_topic_practice_df.select("user_key", "topic_key", "date")
        .unionByName(daily_topic_state_df.select("user_key", "topic_key", "date"))
        .unionByName(daily_topic_feedback_df.select("user_key", "topic_key", "date"))
        .filter(F.col("user_key").isNotNull() & F.col("topic_key").isNotNull() & F.col("date").isNotNull())
        .distinct()
    )
    concept_weakness_base_df = (
        learner_topic_daily_spine_df.alias("spine")
        .join(daily_topic_practice_df.alias("practice"), ["user_key", "topic_key", "date"], "left")
        .join(daily_topic_state_df.alias("state"), ["user_key", "topic_key", "date"], "left")
        .join(daily_topic_feedback_df.alias("feedback"), ["user_key", "topic_key", "date"], "left")
        .select(
            F.col("user_key").cast("int"),
            F.col("topic_key").cast("int"),
            F.col("date"),
            F.col("practice.failure_rate").cast("float"),
            F.col("practice.avg_score").cast("float"),
            F.col("practice.avg_hints_used").cast("float"),
            F.coalesce(F.col("state.repeated_mistake_count"), F.lit(0)).cast("int").alias("repeated_mistake_count"),
            F.coalesce(F.col("feedback.feedback_confidence_score") / 10.0, F.col("state.state_confidence_score")).cast("float").alias("avg_confidence_score"),
            F.col("state.difficulty_score").cast("float"),
        )
    )
    concept_weakness_scored_df = (
        concept_weakness_base_df
        .withColumn(
            "weakness_weight_sum",
            F.when(F.col("failure_rate").isNotNull(), 0.4).otherwise(0.0)
            + F.when(F.col("difficulty_score").isNotNull(), 0.3).otherwise(0.0)
            + F.when(F.col("avg_score").isNotNull(), 0.2).otherwise(0.0)
            + F.when(F.col("avg_confidence_score").isNotNull(), 0.1).otherwise(0.0),
        )
        .withColumn(
            "weakness_weighted_sum",
            F.when(F.col("failure_rate").isNotNull(), F.col("failure_rate") * 0.4).otherwise(0.0)
            + F.when(F.col("difficulty_score").isNotNull(), F.col("difficulty_score") * 0.3).otherwise(0.0)
            + F.when(F.col("avg_score").isNotNull(), (1.0 - F.col("avg_score")) * 0.2).otherwise(0.0)
            + F.when(F.col("avg_confidence_score").isNotNull(), (1.0 - F.col("avg_confidence_score")) * 0.1).otherwise(0.0),
        )
        .withColumn("weakness_sort_score", F.when(F.col("weakness_weight_sum") > 0, F.col("weakness_weighted_sum") / F.col("weakness_weight_sum")))
    )
    weakness_rank_window = Window.partitionBy("user_key", "date").orderBy(F.col("weakness_sort_score").desc_nulls_last(), F.col("topic_key"))
    return (
        concept_weakness_scored_df
        .withColumn("temporary_weakness_rank", F.row_number().over(weakness_rank_window))
        .withColumn("weakness_rank", F.when(F.col("weakness_sort_score").isNotNull(), F.col("temporary_weakness_rank")).otherwise(None).cast("int"))
        .select(
            "user_key",
            "topic_key",
            "date",
            "failure_rate",
            "avg_score",
            "avg_hints_used",
            "repeated_mistake_count",
            "avg_confidence_score",
            "difficulty_score",
            "weakness_rank",
        )
    )


def build_learning_progress(
    fact_attempt_df: DataFrame,
    fact_feedback_df: DataFrame,
    fact_concept_state_df: DataFrame,
) -> DataFrame:
    daily_practice_progress_df = (
        fact_attempt_df
        .groupBy("user_key", F.to_date("attempt_time").alias("date"))
        .agg(
            F.avg("score").cast("float").alias("avg_practice_score"),
            F.count("*").cast("int").alias("total_attempts"),
            F.sum(F.when(F.col("is_correct") == True, 1).otherwise(0)).cast("int").alias("successful_attempts"),
            F.avg(F.when(F.col("hints_used") > 0, 1.0).otherwise(0.0)).cast("float").alias("hint_usage_rate"),
        )
    )
    daily_mastery_progress_df = (
        fact_concept_state_df
        .filter(F.col("is_current") == True)
        .groupBy("user_key", F.to_date("valid_from").alias("date"))
        .agg(F.avg("mastery_score").cast("float").alias("avg_mastery_score"))
    )
    daily_confusion_progress_df = (
        fact_feedback_df
        .filter(F.col("still_confused").isNotNull())
        .groupBy("user_key", F.to_date("feedback_time").alias("date"))
        .agg(F.avg(F.col("still_confused").cast("int")).cast("float").alias("still_confused_rate"))
    )
    progress_daily_spine_df = (
        daily_practice_progress_df.select("user_key", "date")
        .unionByName(daily_mastery_progress_df.select("user_key", "date"))
        .unionByName(daily_confusion_progress_df.select("user_key", "date"))
        .filter(F.col("user_key").isNotNull() & F.col("date").isNotNull())
        .distinct()
    )
    return (
        progress_daily_spine_df.alias("spine")
        .join(daily_practice_progress_df.alias("practice"), ["user_key", "date"], "left")
        .join(daily_mastery_progress_df.alias("mastery"), ["user_key", "date"], "left")
        .join(daily_confusion_progress_df.alias("confusion"), ["user_key", "date"], "left")
        .select(
            F.col("user_key").cast("int"),
            F.col("date"),
            F.col("mastery.avg_mastery_score").cast("float").alias("avg_mastery_score"),
            F.col("practice.avg_practice_score").cast("float").alias("avg_practice_score"),
            F.coalesce(F.col("practice.total_attempts"), F.lit(0)).cast("int").alias("total_attempts"),
            F.coalesce(F.col("practice.successful_attempts"), F.lit(0)).cast("int").alias("successful_attempts"),
            F.col("practice.hint_usage_rate").cast("float").alias("hint_usage_rate"),
            F.col("confusion.still_confused_rate").cast("float").alias("still_confused_rate"),
        )
    )


def build_illusion(fact_attempt_df: DataFrame, fact_feedback_df: DataFrame) -> DataFrame:
    pre_practice_illusion_df = (
        fact_feedback_df
        .filter(F.col("feedback_stage") == "before_practice")
        .select("user_key", "session_id", "practice_id", F.col("confidence_score").cast("int").alias("confidence_before"))
    )
    post_practice_illusion_df = (
        fact_feedback_df
        .filter(F.col("feedback_stage") == "after_practice")
        .select("user_key", "session_id", "practice_id", F.col("confidence_score").cast("int").alias("confidence_after"))
    )
    practice_illusion_df = (
        fact_attempt_df
        .groupBy("user_key", "session_id", "practice_id", "topic_key")
        .agg(F.avg("score").cast("float").alias("practice_score"))
    )
    return (
        pre_practice_illusion_df.alias("pre")
        .join(practice_illusion_df.alias("practice"), ["user_key", "session_id", "practice_id"], "inner")
        .join(post_practice_illusion_df.alias("post"), ["user_key", "session_id", "practice_id"], "inner")
        .select(
            F.col("user_key").cast("int"),
            F.col("practice.topic_key").cast("int"),
            F.col("session_id"),
            F.col("pre.confidence_before").cast("int"),
            F.col("practice.practice_score").cast("float"),
            F.col("post.confidence_after").cast("int"),
            (F.col("pre.confidence_before") - F.col("post.confidence_after")).cast("int").alias("confidence_drop"),
            F.round(F.col("pre.confidence_before") / 10.0 - F.col("practice.practice_score"), 4).cast("float").alias("illusion_gap_score"),
            F.when((F.col("pre.confidence_before") / 10.0 - F.col("practice.practice_score")) >= 0.30, True).otherwise(False).cast("boolean").alias("illusion_flag"),
        )
    )


def build_ml_features(
    fact_attempt_df: DataFrame,
    fact_feedback_df: DataFrame,
    fact_validation_df: DataFrame,
    fact_concept_state_df: DataFrame,
    illusion_df: DataFrame,
) -> DataFrame:
    prediction_sessions_df = (
        fact_attempt_df
        .groupBy("user_key", "topic_key", "session_id")
        .agg(F.min("attempt_time").alias("prediction_time"))
    )
    historical_practice_7d_df = (
        prediction_sessions_df.alias("p")
        .join(
            fact_attempt_df.alias("a"),
            F.expr("""
                a.user_key = p.user_key
                AND a.topic_key = p.topic_key
                AND a.attempt_time < p.prediction_time
                AND a.attempt_time >= p.prediction_time - INTERVAL 7 DAYS
            """),
            "left",
        )
        .groupBy(F.col("p.user_key"), F.col("p.topic_key"), F.col("p.session_id"), F.col("p.prediction_time"))
        .agg(
            F.count("a.attempt_id").cast("int").alias("historical_attempt_count"),
            F.avg("a.score").cast("float").alias("avg_score_last_7_days"),
            F.avg(F.when(F.col("a.is_correct") == False, 1.0).when(F.col("a.is_correct") == True, 0.0)).cast("float").alias("failure_rate_last_7_days"),
            F.sum("a.hints_used").cast("int").alias("hints_used_last_7_days"),
            F.avg("a.attempt_duration_seconds").cast("float").alias("avg_attempt_duration"),
        )
    )
    pre_prediction_feedback_df = (
        prediction_sessions_df.alias("p")
        .join(
            fact_feedback_df.alias("f"),
            F.expr("""
                f.user_key = p.user_key
                AND f.session_id = p.session_id
                AND f.feedback_stage = 'before_practice'
                AND f.feedback_time < p.prediction_time
            """),
            "left",
        )
        .groupBy(F.col("p.user_key"), F.col("p.topic_key"), F.col("p.session_id"), F.col("p.prediction_time"))
        .agg(F.avg("f.confidence_score").cast("float").alias("confidence_before_avg"))
    )
    historical_post_feedback_df = (
        prediction_sessions_df.alias("p")
        .join(
            fact_feedback_df.alias("f"),
            F.expr("""
                f.user_key = p.user_key
                AND f.feedback_stage = 'after_practice'
                AND f.feedback_time < p.prediction_time
                AND f.session_id <> p.session_id
            """),
            "left",
        )
        .groupBy(F.col("p.user_key"), F.col("p.topic_key"), F.col("p.session_id"), F.col("p.prediction_time"))
        .agg(
            F.avg("f.confidence_score").cast("float").alias("confidence_after_avg"),
            F.avg(F.col("f.still_confused").cast("int")).cast("float").alias("still_confused_rate"),
        )
    )
    historical_ai_validation_df = (
        prediction_sessions_df.alias("p")
        .join(
            fact_validation_df.alias("v"),
            F.expr("""
                v.user_key = p.user_key
                AND v.topic_key = p.topic_key
                AND v.validation_time < p.prediction_time
            """),
            "left",
        )
        .groupBy(F.col("p.user_key"), F.col("p.topic_key"), F.col("p.session_id"), F.col("p.prediction_time"))
        .agg(
            F.avg("v.extraction_confidence").cast("float").alias("extraction_confidence_avg"),
            F.avg("v.reliability_score").cast("float").alias("reliability_score_avg"),
        )
    )
    historical_concept_state_candidates_df = (
        prediction_sessions_df.alias("p")
        .join(
            fact_concept_state_df.alias("s"),
            F.expr("""
                s.user_key = p.user_key
                AND s.topic_key = p.topic_key
                AND s.valid_from < p.prediction_time
                AND (
                    s.valid_to IS NULL
                    OR s.valid_to >= p.prediction_time
                )
            """),
            "left",
        )
    )
    historical_state_window = (
        Window
        .partitionBy(F.col("p.user_key"), F.col("p.topic_key"), F.col("p.session_id"))
        .orderBy(F.col("s.valid_from").desc_nulls_last(), F.col("s.state_version").desc_nulls_last())
    )
    historical_concept_state_df = (
        historical_concept_state_candidates_df
        .withColumn("state_row_number", F.row_number().over(historical_state_window))
        .filter(F.col("state_row_number") == 1)
        .select(
            F.col("p.user_key"),
            F.col("p.topic_key"),
            F.col("p.session_id"),
            F.col("p.prediction_time"),
            F.col("s.mastery_score").cast("float").alias("historical_mastery_score"),
            F.col("s.difficulty_score").cast("float").alias("historical_difficulty_score"),
            F.col("s.repeated_mistake_count").cast("int").alias("repeated_mistake_count"),
        )
    )
    practice_session_times_df = (
        fact_attempt_df
        .groupBy("user_key", "topic_key", "session_id")
        .agg(F.min("attempt_time").alias("historical_session_time"))
    )
    illusion_with_time_df = (
        illusion_df.alias("i")
        .join(practice_session_times_df.alias("t"), ["user_key", "topic_key", "session_id"], "inner")
        .select("user_key", "topic_key", "session_id", "illusion_gap_score", "historical_session_time")
    )
    historical_illusion_df = (
        prediction_sessions_df.alias("p")
        .join(
            illusion_with_time_df.alias("i"),
            F.expr("""
                i.user_key = p.user_key
                AND i.topic_key = p.topic_key
                AND i.session_id <> p.session_id
                AND i.historical_session_time < p.prediction_time
            """),
            "left",
        )
        .groupBy(F.col("p.user_key"), F.col("p.topic_key"), F.col("p.session_id"), F.col("p.prediction_time"))
        .agg(F.avg("i.illusion_gap_score").cast("float").alias("illusion_gap_score"))
    )
    return (
        prediction_sessions_df.alias("p")
        .join(historical_practice_7d_df.alias("practice"), ["user_key", "topic_key", "session_id", "prediction_time"], "left")
        .join(pre_prediction_feedback_df.alias("pre"), ["user_key", "topic_key", "session_id", "prediction_time"], "left")
        .join(historical_post_feedback_df.alias("post"), ["user_key", "topic_key", "session_id", "prediction_time"], "left")
        .join(historical_illusion_df.alias("illusion"), ["user_key", "topic_key", "session_id", "prediction_time"], "left")
        .join(historical_concept_state_df.alias("state"), ["user_key", "topic_key", "session_id", "prediction_time"], "left")
        .join(historical_ai_validation_df.alias("ai"), ["user_key", "topic_key", "session_id", "prediction_time"], "left")
        .select(
            F.col("user_key").cast("int"),
            F.col("topic_key").cast("int"),
            F.col("session_id"),
            F.col("practice.avg_score_last_7_days").cast("float"),
            F.col("practice.failure_rate_last_7_days").cast("float"),
            F.coalesce(F.col("practice.hints_used_last_7_days"), F.lit(0)).cast("int").alias("hints_used_last_7_days"),
            F.col("practice.avg_attempt_duration").cast("float"),
            F.col("pre.confidence_before_avg").cast("float"),
            F.col("post.confidence_after_avg").cast("float"),
            F.col("post.still_confused_rate").cast("float"),
            F.col("illusion.illusion_gap_score").cast("float"),
            F.coalesce(F.col("state.repeated_mistake_count"), F.lit(0)).cast("int").alias("repeated_mistake_count"),
            F.col("ai.extraction_confidence_avg").cast("float"),
            F.col("ai.reliability_score_avg").cast("float"),
            F.lit(None).cast("float").alias("overall_motivation_avg"),
            F.lit(None).cast("float").alias("overall_stress_avg"),
            F.lit(None).cast("float").alias("topic_self_reported_understanding_avg"),
            F.col("pre.confidence_before_avg").cast("float").alias("topic_confidence_avg"),
        )
    )


def run_job(spark: SparkSession) -> int:
    logger.info("gold_aggregations_job started.")
    fact_interaction_df = spark.table(FACT_INTERACTION)
    fact_attempt_df = spark.table(FACT_ATTEMPT)
    fact_session_df = spark.table(FACT_SESSION)
    fact_feedback_df = spark.table(FACT_FEEDBACK)
    fact_validation_df = spark.table(FACT_VALIDATION)
    fact_concept_state_df = spark.table(FACT_CONCEPT_STATE)

    overview_df = valid_and_single_row(
        build_learner_overview(fact_interaction_df, fact_attempt_df, fact_session_df, fact_feedback_df, fact_validation_df, fact_concept_state_df),
        ["user_key", "date"],
        "agg_learner_overview_daily",
    )
    merge_dataframe(
        spark,
        overview_df,
        AGG_OVERVIEW,
        ["user_key", "date"],
        [
            "total_sessions",
            "total_learning_events",
            "topics_practiced",
            "weak_topics_count",
            "repeated_mistakes_count",
            "avg_mastery_score",
            "at_risk_topics_count",
            "avg_reliability_score",
            "avg_motivation_score",
            "avg_stress_score",
            "avg_self_reported_understanding",
        ],
        overview_df.columns,
        "gold_aggregations_overview_src",
    )

    weakness_df = valid_and_single_row(
        build_concept_weakness(fact_attempt_df, fact_feedback_df, fact_concept_state_df),
        ["user_key", "topic_key", "date"],
        "agg_concept_weakness_daily",
    )
    merge_dataframe(
        spark,
        weakness_df,
        AGG_WEAKNESS,
        ["user_key", "topic_key", "date"],
        ["failure_rate", "avg_score", "avg_hints_used", "repeated_mistake_count", "avg_confidence_score", "difficulty_score", "weakness_rank"],
        weakness_df.columns,
        "gold_aggregations_weakness_src",
    )

    progress_df = valid_and_single_row(
        build_learning_progress(fact_attempt_df, fact_feedback_df, fact_concept_state_df),
        ["user_key", "date"],
        "agg_learning_progress_daily",
    )
    merge_dataframe(
        spark,
        progress_df,
        AGG_PROGRESS,
        ["user_key", "date"],
        ["avg_mastery_score", "avg_practice_score", "total_attempts", "successful_attempts", "hint_usage_rate", "still_confused_rate"],
        progress_df.columns,
        "gold_aggregations_progress_src",
    )

    illusion_df = valid_and_single_row(
        build_illusion(fact_attempt_df, fact_feedback_df),
        ["user_key", "topic_key", "session_id"],
        "agg_illusion_of_learning",
    )
    merge_dataframe(
        spark,
        illusion_df,
        AGG_ILLUSION,
        ["user_key", "topic_key", "session_id"],
        ["confidence_before", "practice_score", "confidence_after", "confidence_drop", "illusion_gap_score", "illusion_flag"],
        illusion_df.columns,
        "gold_aggregations_illusion_src",
    )

    features_df = valid_and_single_row(
        build_ml_features(fact_attempt_df, fact_feedback_df, fact_validation_df, fact_concept_state_df, illusion_df),
        ["user_key", "topic_key", "session_id"],
        "ml_learning_difficulty_features",
    )
    merge_dataframe(
        spark,
        features_df,
        ML_FEATURES,
        ["user_key", "topic_key", "session_id"],
        [
            "avg_score_last_7_days",
            "failure_rate_last_7_days",
            "hints_used_last_7_days",
            "avg_attempt_duration",
            "confidence_before_avg",
            "confidence_after_avg",
            "still_confused_rate",
            "illusion_gap_score",
            "repeated_mistake_count",
            "extraction_confidence_avg",
            "reliability_score_avg",
            "overall_motivation_avg",
            "overall_stress_avg",
            "topic_self_reported_understanding_avg",
            "topic_confidence_avg",
        ],
        features_df.columns,
        "gold_aggregations_ml_features_src",
    )

    logger.info("gold_aggregations_job completed successfully.")
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("gold_aggregations_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
