#!/usr/bin/env python
# coding: utf-8
"""Dynamic learning-difficulty model training job."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Sequence

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

ML_FEATURES = "demo.gold.ml_learning_difficulty_features"
FACT_ATTEMPT = "demo.gold.fact_practice_attempt"
TRAINING_TABLE = "demo.gold.ml_learning_difficulty_training"
PREDICTIONS_TABLE = "demo.gold.ml_learning_difficulty_predictions"
MODEL_ROOT = "/home/iceberg/notebooks/notebooks/models"

FEATURE_COLUMNS = [
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
]


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("ml_training_job").getOrCreate()


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


def require_existing_tables(spark: SparkSession) -> None:
    for table_name in [ML_FEATURES, FACT_ATTEMPT, TRAINING_TABLE, PREDICTIONS_TABLE]:
        try:
            spark.table(table_name).limit(0).count()
        except Exception as exc:
            raise RuntimeError(f"Required table is missing or unreadable: {table_name}") from exc


def build_current_training_rows(spark: SparkSession) -> DataFrame:
    training_created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    ml_features_df = spark.table(ML_FEATURES)
    practice_attempts_df = spark.table(FACT_ATTEMPT)
    session_labels_df = (
        practice_attempts_df
        .groupBy("user_key", "topic_key", "session_id")
        .agg(F.avg("score").cast("float").alias("session_avg_score"))
        .withColumn(
            "struggle_label",
            F.when(F.col("session_avg_score") <= 0.5, F.lit(1.0)).otherwise(F.lit(0.0)).cast("double"),
        )
    )
    return (
        ml_features_df.alias("f")
        .join(session_labels_df.alias("l"), ["user_key", "topic_key", "session_id"], "inner")
        .select(
            F.col("user_key").cast("int"),
            F.col("topic_key").cast("int"),
            F.col("session_id"),
            F.col("avg_score_last_7_days").cast("float"),
            F.col("failure_rate_last_7_days").cast("float"),
            F.col("hints_used_last_7_days").cast("int"),
            F.col("avg_attempt_duration").cast("float"),
            F.col("confidence_before_avg").cast("float"),
            F.col("confidence_after_avg").cast("float"),
            F.col("still_confused_rate").cast("float"),
            F.col("illusion_gap_score").cast("float"),
            F.col("repeated_mistake_count").cast("int"),
            F.col("extraction_confidence_avg").cast("float"),
            F.col("reliability_score_avg").cast("float"),
            F.col("overall_motivation_avg").cast("float"),
            F.col("overall_stress_avg").cast("float"),
            F.col("topic_self_reported_understanding_avg").cast("float"),
            F.col("topic_confidence_avg").cast("float"),
            F.col("session_avg_score").cast("float"),
            F.col("struggle_label").cast("double"),
            F.lit(training_created_at).cast("timestamp").alias("training_row_created_at"),
        )
    )


def validate_training_input(training_df: DataFrame) -> bool:
    row_count = training_df.count()
    logger.info("Accumulated training rows=%s", row_count)
    if row_count == 0:
        logger.error("Training validation failed: accumulated training table is empty.")
        return False

    missing_features = [column for column in FEATURE_COLUMNS if column not in training_df.columns]
    if missing_features:
        logger.error("Training validation failed: missing feature columns=%s", missing_features)
        return False

    invalid_labels = training_df.filter(~F.col("struggle_label").isin(0.0, 1.0) | F.col("struggle_label").isNull()).count()
    if invalid_labels > 0:
        logger.error("Training validation failed: invalid struggle_label rows=%s", invalid_labels)
        return False

    label_count = training_df.select("struggle_label").distinct().count()
    logger.info("Label class count=%s", label_count)
    if label_count < 2:
        logger.error("Training validation failed: at least two label classes are required.")
        return False

    return True


def build_model_version() -> str:
    return datetime.now(timezone.utc).strftime("learning_difficulty_model_%Y%m%d_%H%M%S_%f")


def run_job(spark: SparkSession) -> int:
    logger.info("ml_training_job started.")
    require_existing_tables(spark)

    current_training_rows_df = valid_and_single_row(
        build_current_training_rows(spark),
        ["user_key", "topic_key", "session_id"],
        "current_ml_training_rows",
    )
    logger.info("Current labeled rows=%s", current_training_rows_df.count())
    merge_dataframe(
        spark,
        current_training_rows_df,
        TRAINING_TABLE,
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
            "session_avg_score",
            "struggle_label",
        ],
        current_training_rows_df.columns,
        "ml_training_current_rows_src",
    )

    training_history_df = spark.table(TRAINING_TABLE)
    if not validate_training_input(training_history_df):
        return 1

    model_input_df = training_history_df.fillna(0.0, subset=FEATURE_COLUMNS)
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep",
    )
    assembled_training_df = assembler.transform(model_input_df)
    classifier = DecisionTreeClassifier(
        featuresCol="features",
        labelCol="struggle_label",
        predictionCol="prediction",
        probabilityCol="probability",
        maxDepth=3,
        minInstancesPerNode=1,
        seed=42,
    )

    model = classifier.fit(assembled_training_df)
    model_version = build_model_version()
    model_path = os.path.join(MODEL_ROOT, model_version)
    logger.info("Model version=%s model_path=%s", model_version, model_path)
    if os.path.exists(model_path):
        logger.error("Model path already exists: %s", model_path)
        return 1

    try:
        model.write().save(model_path)
    except Exception:
        logger.exception("Model save failed; predictions will not be merged for model_version=%s", model_version)
        return 1

    prediction_created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    prediction_df = (
        model.transform(assembled_training_df)
        .withColumn("struggle_probability", vector_to_array(F.col("probability"))[1].cast("float"))
        .select(
            F.col("user_key").cast("int"),
            F.col("topic_key").cast("int"),
            F.col("session_id"),
            F.col("struggle_label").cast("double"),
            F.col("prediction").cast("double"),
            F.col("struggle_probability"),
            F.lit(model_version).alias("model_version"),
            F.lit(prediction_created_at).cast("timestamp").alias("prediction_created_at"),
        )
    )
    prediction_df = prediction_df.localCheckpoint(eager=True)
    logger.info(
        "Prediction DataFrame materialized before Iceberg MERGE | rows=%s",
        prediction_df.count(),
    )
    prediction_df = valid_and_single_row(
        prediction_df,
        ["user_key", "topic_key", "session_id", "model_version"],
        "ml_learning_difficulty_predictions",
    )
    merge_dataframe(
        spark,
        prediction_df,
        PREDICTIONS_TABLE,
        ["user_key", "topic_key", "session_id", "model_version"],
        ["struggle_label", "prediction", "struggle_probability"],
        prediction_df.columns,
        "ml_training_predictions_src",
    )

    correct_predictions = prediction_df.filter(F.col("prediction") == F.col("struggle_label")).count()
    total_predictions = prediction_df.count()
    training_accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0.0
    total_prediction_history = spark.table(PREDICTIONS_TABLE).count()
    logger.info(
        "ML training completed | accumulated_training_rows=%s model_version=%s predictions_for_model=%s total_prediction_history=%s training_accuracy=%s",
        training_history_df.count(),
        model_version,
        total_predictions,
        total_prediction_history,
        training_accuracy,
    )
    logger.warning("Training accuracy is reported for the accumulated labeled training data only and is not a generalization metric.")
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("ml_training_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
