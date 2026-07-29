"""Idempotent Gold table setup for the ALI Iceberg environment."""

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
        "demo.gold.dim_learner",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.dim_learner (
            user_key INT,
            user_id STRING,
            registration_date DATE,
            preferred_language STRING,
            background_level STRING,
            learning_goal STRING,
            main_domain STRING,
            profile_updated_at TIMESTAMP,
            is_active BOOLEAN,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.dim_topic",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.dim_topic (
            topic_key INT,
            taxonomy_id STRING,
            topic_id STRING,
            topic_name STRING,
            normalized_topic_name STRING,
            domain STRING,
            parent_topic_key INT,
            taxonomy_level STRING,
            first_detected_at TIMESTAMP,
            validation_status STRING,
            is_active BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.dim_content_type",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.dim_content_type (
            content_type_key INT,
            content_type_id STRING,
            content_type_name STRING,
            category STRING,
            academic_orientation STRING,
            requires_logic BOOLEAN,
            requires_numerical_reasoning BOOLEAN,
            requires_text_interpretation BOOLEAN,
            requires_visual_reasoning BOOLEAN,
            requires_memorization BOOLEAN,
            description STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.dim_reference_source",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.dim_reference_source (
            reference_key INT,
            reference_id STRING,
            source_name STRING,
            source_type STRING,
            file_name STRING,
            reliability_level STRING,
            domain STRING,
            is_active BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.fact_learning_interaction",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.fact_learning_interaction (
            interaction_key STRING,
            event_id STRING,
            user_key INT,
            session_id STRING,
            topic_key INT,
            event_time TIMESTAMP,
            event_date DATE,
            event_hour INT,
            event_type STRING,
            source_system STRING,
            is_practice_event BOOLEAN,
            attempt_id STRING,
            question_id STRING,
            is_correct BOOLEAN,
            score FLOAT,
            hints_used INT,
            attempt_duration_seconds INT,
            attempt_number INT
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.fact_practice_attempt",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.fact_practice_attempt (
            attempt_key STRING,
            attempt_id STRING,
            event_id STRING,
            user_key INT,
            session_id STRING,
            topic_key INT,
            practice_id STRING,
            question_id STRING,
            question_version INT,
            attempt_time TIMESTAMP,
            selected_option_letter STRING,
            is_correct BOOLEAN,
            score FLOAT,
            hints_used INT,
            attempt_duration_seconds INT,
            attempt_number INT
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.fact_learning_session",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.fact_learning_session (
            session_id STRING,
            user_key INT,
            session_start_time TIMESTAMP,
            session_end_time TIMESTAMP,
            session_duration_minutes INT,
            main_source_system STRING,
            total_events INT,
            total_attempts INT,
            total_hints_used INT,
            average_practice_score FLOAT,
            session_status STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.fact_learning_feedback",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.fact_learning_feedback (
            feedback_key STRING,
            user_key INT,
            session_id STRING,
            topic_key INT,
            practice_id STRING,
            feedback_stage STRING,
            feedback_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            delay_minutes INT,
            confidence_score INT,
            perceived_understanding_score INT,
            perceived_difficulty_score INT,
            motivation_score INT,
            stress_score INT,
            still_confused BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.fact_ai_insight_validation",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.fact_ai_insight_validation (
            validation_key STRING,
            validation_id STRING,
            insight_id STRING,
            event_id STRING,
            user_key INT,
            session_id STRING,
            topic_key INT,
            reference_key INT,
            dynamic_concept_name STRING,
            extracted_at TIMESTAMP,
            validation_time TIMESTAMP,
            extraction_confidence FLOAT,
            semantic_match_score FLOAT,
            reliability_score FLOAT,
            contradiction_flag BOOLEAN,
            validation_status STRING,
            match_rank INT
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.fact_learner_concept_state",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.fact_learner_concept_state (
            concept_state_key STRING,
            user_key INT,
            topic_key INT,
            state_version INT,
            mastery_score FLOAT,
            difficulty_score FLOAT,
            confidence_score FLOAT,
            repeated_mistake_count INT,
            last_practice_score FLOAT,
            struggle_risk_score FLOAT,
            evidence_count INT,
            last_evidence_time TIMESTAMP,
            state_calculation_version STRING,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.agg_learner_overview_daily",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.agg_learner_overview_daily (
            user_key INT,
            date DATE,
            total_sessions INT,
            total_learning_events INT,
            topics_practiced INT,
            weak_topics_count INT,
            repeated_mistakes_count INT,
            avg_mastery_score FLOAT,
            at_risk_topics_count INT,
            avg_reliability_score FLOAT,
            avg_motivation_score FLOAT,
            avg_stress_score FLOAT,
            avg_self_reported_understanding FLOAT
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.agg_concept_weakness_daily",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.agg_concept_weakness_daily (
            user_key INT,
            topic_key INT,
            date DATE,
            failure_rate FLOAT,
            avg_score FLOAT,
            avg_hints_used FLOAT,
            repeated_mistake_count INT,
            avg_confidence_score FLOAT,
            difficulty_score FLOAT,
            weakness_rank INT
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.agg_learning_progress_daily",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.agg_learning_progress_daily (
            user_key INT,
            date DATE,
            avg_mastery_score FLOAT,
            avg_practice_score FLOAT,
            total_attempts INT,
            successful_attempts INT,
            hint_usage_rate FLOAT,
            still_confused_rate FLOAT
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.agg_illusion_of_learning",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.agg_illusion_of_learning (
            user_key INT,
            topic_key INT,
            session_id STRING,
            confidence_before INT,
            practice_score FLOAT,
            confidence_after INT,
            confidence_drop INT,
            illusion_gap_score FLOAT,
            illusion_flag BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.gold.ml_learning_difficulty_features",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.ml_learning_difficulty_features (
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
            topic_confidence_avg FLOAT
        )
        USING iceberg
        """,
    ),
]


def run_job() -> int:
    spark = (
        SparkSession.builder.appName("setup_gold_tables_job")
        .getOrCreate()
    )

    try:
        for table_name, ddl in TABLE_DDLS:
            logger.info("Creating or verifying Gold table | table=%s", table_name)
            spark.sql(ddl)

        logger.info("Gold table setup completed | table_count=%s", len(TABLE_DDLS))
        return 0
    except Exception:
        logger.exception("Gold table setup failed.")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_job())
