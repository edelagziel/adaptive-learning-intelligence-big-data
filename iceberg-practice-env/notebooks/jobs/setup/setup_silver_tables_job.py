"""Idempotent Silver table setup for the ALI Iceberg environment."""

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
        "demo.silver.learner_profiles",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.learner_profiles (
            user_id STRING,
            registration_date DATE,
            preferred_language STRING,
            background_level STRING,
            learning_goal STRING,
            main_domain STRING,
            profile_updated_at TIMESTAMP,
            ingestion_time TIMESTAMP,
            is_current BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.question_bank",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.question_bank (
            question_id STRING,
            question_version INT,
            question_text STRING,
            question_type STRING,
            domain STRING,
            topic STRING,
            subtopic STRING,
            difficulty_level INT,
            option_a_text STRING,
            option_b_text STRING,
            option_c_text STRING,
            option_d_text STRING,
            correct_option_letter STRING,
            created_by STRING,
            generation_model STRING,
            created_at TIMESTAMP,
            is_active BOOLEAN,
            validation_status STRING,
            content_hash STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.reference_materials",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.reference_materials (
            reference_id STRING,
            batch_id STRING,
            source_type STRING,
            source_name STRING,
            file_name STRING,
            import_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            domain STRING,
            title STRING,
            topic STRING,
            content_text STRING,
            reliability_level STRING,
            author_or_owner STRING,
            content_hash STRING,
            is_active BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.learning_events",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.learning_events (
            event_id STRING,
            user_id STRING,
            session_id STRING,
            event_type STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            source_system STRING,
            event_date DATE,
            event_hour INT,
            payload_valid BOOLEAN,
            processing_status STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.practice_attempts",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.practice_attempts (
            attempt_id STRING,
            event_id STRING,
            user_id STRING,
            session_id STRING,
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
        "demo.silver.pre_practice_feedback",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.pre_practice_feedback (
            feedback_id STRING,
            user_id STRING,
            session_id STRING,
            practice_id STRING,
            feedback_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            delay_minutes INT,
            confidence_before_score INT,
            perceived_understanding_before_score INT,
            expected_difficulty_score INT,
            free_text_before STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.post_practice_feedback",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.post_practice_feedback (
            feedback_id STRING,
            user_id STRING,
            session_id STRING,
            practice_id STRING,
            feedback_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            delay_minutes INT,
            confidence_after_score INT,
            perceived_understanding_after_score INT,
            perceived_difficulty_score INT,
            still_confused BOOLEAN,
            free_text_after STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.learner_check_in",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.learner_check_in (
            feedback_id STRING,
            user_id STRING,
            session_id STRING,
            feedback_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            delay_minutes INT,
            overall_confidence_score INT,
            overall_motivation_score INT,
            overall_stress_score INT,
            free_text STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.learner_check_in_topics",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.learner_check_in_topics (
            feedback_id STRING,
            user_id STRING,
            session_id STRING,
            topic_id STRING,
            feedback_time TIMESTAMP,
            perceived_understanding_score INT,
            topic_confidence_score INT,
            still_confused BOOLEAN
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.ai_extracted_insights",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.ai_extracted_insights (
            insight_id STRING,
            event_id STRING,
            user_id STRING,
            session_id STRING,
            extracted_at TIMESTAMP,
            dynamic_concept_name STRING,
            extraction_confidence FLOAT,
            processing_model STRING,
            validation_status STRING,
            ai_attributes STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.validated_learning_insights",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.validated_learning_insights (
            validation_id STRING,
            insight_id STRING,
            reference_id STRING,
            user_id STRING,
            session_id STRING,
            dynamic_concept_name STRING,
            validation_time TIMESTAMP,
            semantic_match_score FLOAT,
            reliability_score FLOAT,
            contradiction_flag BOOLEAN,
            validation_notes STRING,
            validation_status STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.content_taxonomy",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.content_taxonomy (
            taxonomy_id STRING,
            domain STRING,
            topic STRING,
            subtopic STRING,
            concept_name STRING,
            normalized_domain STRING,
            normalized_topic STRING,
            normalized_subtopic STRING,
            normalized_concept_name STRING,
            taxonomy_level STRING,
            parent_taxonomy_id STRING,
            source_type STRING,
            first_detected_at TIMESTAMP,
            last_detected_at TIMESTAMP,
            validation_status STRING,
            is_active BOOLEAN,
            taxonomy_hash STRING
        )
        USING iceberg
        """,
    ),
    (
        "demo.silver.learner_concept_evidence",
        """
        CREATE TABLE IF NOT EXISTS demo.silver.learner_concept_evidence (
            evidence_id STRING,
            user_id STRING,
            session_id STRING,
            taxonomy_id STRING,
            event_id STRING,
            attempt_id STRING,
            feedback_id STRING,
            insight_id STRING,
            validation_id STRING,
            evidence_type STRING,
            evidence_time TIMESTAMP,
            is_correct BOOLEAN,
            score FLOAT,
            hints_used INT,
            attempt_duration_seconds INT,
            attempt_number INT,
            extraction_confidence FLOAT,
            semantic_match_score FLOAT,
            reliability_score FLOAT,
            contradiction_flag BOOLEAN,
            confidence_score INT,
            perceived_understanding_score INT,
            perceived_difficulty_score INT,
            still_confused BOOLEAN,
            source_table STRING,
            processing_time TIMESTAMP
        )
        USING iceberg
        """,
    ),
]


def run_job() -> int:
    spark = (
        SparkSession.builder.appName("setup_silver_tables_job")
        .getOrCreate()
    )

    try:
        for table_name, ddl in TABLE_DDLS:
            logger.info("Creating or verifying Silver table | table=%s", table_name)
            spark.sql(ddl)

        logger.info("Silver table setup completed | table_count=%s", len(TABLE_DDLS))
        return 0
    except Exception:
        logger.exception("Silver table setup failed.")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_job())
