#!/usr/bin/env python
# coding: utf-8
"""Read-only export job for the ALI demo dashboard."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


OUTPUT_PATH = "/home/iceberg/notebooks/notebooks/dashboard/data/dashboard_data.json"

DIM_LEARNER = "demo.gold.dim_learner"
DIM_TOPIC = "demo.gold.dim_topic"
FACT_INTERACTION = "demo.gold.fact_learning_interaction"
FACT_ATTEMPT = "demo.gold.fact_practice_attempt"
FACT_CONCEPT_STATE = "demo.gold.fact_learner_concept_state"
AGG_PROGRESS = "demo.gold.agg_learning_progress_daily"
AGG_ILLUSION = "demo.gold.agg_illusion_of_learning"
ML_FEATURES = "demo.gold.ml_learning_difficulty_features"
ML_PREDICTIONS = "demo.gold.ml_learning_difficulty_predictions"
BRONZE_QUALITY_RESULTS = "demo.quality.bronze_quality_results"
SILVER_QUALITY_RESULTS = "demo.quality.silver_quality_results"
BRONZE_QUARANTINE = "demo.quality.bronze_quarantine"
SILVER_QUARANTINE = "demo.quality.silver_quarantine"

TOP_N = 8
OPEN_STATUS_VALUES = {"OPEN", "UNRESOLVED", "ACTIVE", "PENDING"}


class DashboardExporter:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.tables_read: List[str] = []
        self.unavailable: Dict[str, str] = {}

    def table(self, table_name: str, required_columns: Sequence[str]) -> Optional[DataFrame]:
        try:
            df = self.spark.table(table_name)
            missing_columns = [column for column in required_columns if column not in df.columns]
            if missing_columns:
                self.unavailable[table_name] = f"Missing columns: {', '.join(missing_columns)}"
                logger.warning("Table unavailable for dashboard section | table=%s missing_columns=%s", table_name, missing_columns)
                return None

            df.limit(0).count()
            if table_name not in self.tables_read:
                self.tables_read.append(table_name)
            return df
        except Exception as exc:
            self.unavailable[table_name] = str(exc)
            logger.warning("Table unavailable for dashboard section | table=%s error=%s", table_name, exc)
            return None


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("export_dashboard_data_job").getOrCreate()


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat() + "Z"
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def first_value(df: Optional[DataFrame], expression: F.Column, alias: str) -> Any:
    if df is None:
        return None
    row = df.agg(expression.alias(alias)).first()
    return None if row is None else row[alias]


def rows_as_dicts(df: Optional[DataFrame], limit: int = TOP_N) -> List[Dict[str, Any]]:
    if df is None:
        return []
    return [json_safe(row.asDict(recursive=True)) for row in df.limit(limit).collect()]


def stable_row_hash(columns: Sequence[str]) -> F.Column:
    return F.sha2(F.to_json(F.struct(*[F.col(column) for column in columns])), 256)


def latest_predictions(predictions_df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
    non_null_timestamp_count = predictions_df.filter(F.col("prediction_created_at").isNotNull()).count()
    fallback_note = None

    if non_null_timestamp_count > 0:
        order_columns = [
            F.col("prediction_created_at").desc_nulls_last(),
            F.col("model_version").desc_nulls_last(),
            stable_row_hash(predictions_df.columns).asc(),
        ]
        null_only_keys_df = (
            predictions_df
            .groupBy("user_key", "topic_key", "session_id")
            .agg(F.max(F.when(F.col("prediction_created_at").isNotNull(), 1).otherwise(0)).alias("has_timestamp"))
            .filter(F.col("has_timestamp") == 0)
        )
        fallback_key_count = null_only_keys_df.count()
        if fallback_key_count > 0:
            fallback_note = (
                "prediction_created_at was null for some learner/topic/session keys; "
                "those keys used model_version DESC and a stable row hash as a deterministic fallback."
            )
    else:
        order_columns = [
            F.col("model_version").desc_nulls_last(),
            stable_row_hash(predictions_df.columns).asc(),
        ]
        fallback_key_count = predictions_df.select("user_key", "topic_key", "session_id").distinct().count()
        fallback_note = (
            "prediction_created_at was null or unavailable for all prediction records; "
            "latest rows used model_version DESC and a stable row hash as a deterministic fallback."
        )

    window = Window.partitionBy("user_key", "topic_key", "session_id").orderBy(*order_columns)
    latest_df = (
        predictions_df
        .withColumn("_prediction_row_number", F.row_number().over(window))
        .filter(F.col("_prediction_row_number") == 1)
        .drop("_prediction_row_number")
    )
    metadata = {
        "selection_rule": "row_number over user_key, topic_key, session_id ordered by prediction_created_at DESC, then model_version DESC, then stable row hash",
        "source_rows": predictions_df.count(),
        "latest_rows": latest_df.count(),
        "fallback_key_count": int(fallback_key_count),
        "fallback_note": fallback_note,
    }
    return latest_df, metadata


def dedupe_features(features_df: DataFrame) -> DataFrame:
    window = (
        Window
        .partitionBy("user_key", "topic_key", "session_id")
        .orderBy(stable_row_hash(features_df.columns).asc())
    )
    return (
        features_df
        .withColumn("_feature_row_number", F.row_number().over(window))
        .filter(F.col("_feature_row_number") == 1)
        .drop("_feature_row_number")
    )


def risk_level(score_column: F.Column) -> F.Column:
    return (
        F.when(score_column >= 0.75, F.lit("High"))
        .when(score_column >= 0.50, F.lit("Medium"))
        .otherwise(F.lit("Low"))
    )


def build_kpis(exporter: DashboardExporter, latest_prediction_df: Optional[DataFrame]) -> Dict[str, Any]:
    dim_learner_df = exporter.table(DIM_LEARNER, ["user_key", "is_current"])
    interaction_df = exporter.table(FACT_INTERACTION, ["event_id"])
    attempt_df = exporter.table(FACT_ATTEMPT, ["attempt_key"])
    concept_state_df = exporter.table(FACT_CONCEPT_STATE, ["topic_key", "user_key", "mastery_score", "is_current"])

    current_learners_df = None if dim_learner_df is None else dim_learner_df.filter(F.col("is_current") == True)
    current_state_df = None if concept_state_df is None else concept_state_df.filter(F.col("is_current") == True)
    weak_state_df = None if current_state_df is None else current_state_df.filter(F.col("mastery_score") < 0.6)

    at_risk_learners = None
    if latest_prediction_df is not None:
        at_risk_learners = first_value(
            latest_prediction_df.filter(F.col("struggle_probability") >= 0.6),
            F.countDistinct("user_key"),
            "value",
        )

    return {
        "total_learners": first_value(current_learners_df, F.countDistinct("user_key"), "value"),
        "total_learning_events": first_value(interaction_df, F.countDistinct("event_id"), "value"),
        "total_practice_attempts": first_value(attempt_df, F.countDistinct("attempt_key"), "value"),
        "weak_concepts": first_value(weak_state_df, F.countDistinct("topic_key"), "value"),
        "average_mastery": first_value(current_state_df, F.avg("mastery_score"), "value"),
        "at_risk_learners": at_risk_learners,
    }


def build_weakest_concepts(exporter: DashboardExporter) -> List[Dict[str, Any]]:
    concept_state_df = exporter.table(FACT_CONCEPT_STATE, ["topic_key", "user_key", "mastery_score", "is_current"])
    topic_df = exporter.table(DIM_TOPIC, ["topic_key", "topic_name", "taxonomy_level"])
    if concept_state_df is None:
        return []

    base_df = (
        concept_state_df
        .filter((F.col("is_current") == True) & F.col("mastery_score").isNotNull())
        .groupBy("topic_key")
        .agg(
            F.avg("mastery_score").cast("float").alias("mastery_score"),
            F.countDistinct("user_key").cast("int").alias("learner_count"),
        )
    )
    if topic_df is not None:
        base_df = (
            base_df.alias("w")
            .join(topic_df.select("topic_key", "topic_name").alias("t"), "topic_key", "left")
            .select(
                F.coalesce(F.col("t.topic_name"), F.concat(F.lit("Topic "), F.col("w.topic_key").cast("string"))).alias("concept"),
                F.col("w.mastery_score"),
                F.col("w.learner_count"),
            )
        )
    else:
        base_df = base_df.select(
            F.concat(F.lit("Topic "), F.col("topic_key").cast("string")).alias("concept"),
            "mastery_score",
            "learner_count",
        )

    return rows_as_dicts(base_df.orderBy(F.col("mastery_score").asc_nulls_last(), F.col("concept")), TOP_N)


def build_learning_progress(exporter: DashboardExporter) -> List[Dict[str, Any]]:
    progress_df = exporter.table(AGG_PROGRESS, ["date", "avg_mastery_score", "avg_practice_score", "total_attempts"])
    if progress_df is None:
        return []

    return rows_as_dicts(
        progress_df
        .groupBy("date")
        .agg(
            F.avg("avg_practice_score").cast("float").alias("average_score"),
            F.avg("avg_mastery_score").cast("float").alias("average_mastery"),
            F.sum("total_attempts").cast("int").alias("event_count"),
        )
        .select(F.col("date").cast("string").alias("period"), "average_score", "average_mastery", "event_count")
        .orderBy("period"),
        30,
    )


def build_learning_gap(exporter: DashboardExporter) -> List[Dict[str, Any]]:
    illusion_df = exporter.table(
        AGG_ILLUSION,
        ["user_key", "topic_key", "session_id", "confidence_before", "practice_score", "confidence_after", "illusion_gap_score", "illusion_flag"],
    )
    topic_df = exporter.table(DIM_TOPIC, ["topic_key", "topic_name"])
    if illusion_df is None:
        return []

    base_df = illusion_df
    if topic_df is not None:
        base_df = (
            base_df.alias("i")
            .join(topic_df.select("topic_key", "topic_name").alias("t"), "topic_key", "left")
            .select(
                F.coalesce(F.col("t.topic_name"), F.concat(F.lit("Topic "), F.col("i.topic_key").cast("string"))).alias("concept"),
                F.col("i.confidence_before"),
                F.col("i.practice_score"),
                F.col("i.confidence_after"),
                F.col("i.illusion_gap_score"),
                F.col("i.illusion_flag"),
            )
        )
    else:
        base_df = base_df.select(
            F.concat(F.lit("Topic "), F.col("topic_key").cast("string")).alias("concept"),
            "confidence_before",
            "practice_score",
            "confidence_after",
            "illusion_gap_score",
            "illusion_flag",
        )

    return rows_as_dicts(
        base_df
        .withColumn(
            "gap_level",
            F.when(F.col("illusion_gap_score") >= 0.3, F.lit("High"))
            .when(F.col("illusion_gap_score") >= 0.15, F.lit("Medium"))
            .otherwise(F.lit("Low")),
        )
        .orderBy(F.col("illusion_gap_score").desc_nulls_last(), F.col("concept"))
        .select("concept", "confidence_before", "practice_score", "confidence_after", "illusion_gap_score", "gap_level"),
        TOP_N,
    )


def build_learner_risk(
    exporter: DashboardExporter,
    latest_prediction_df: Optional[DataFrame],
) -> Tuple[List[Dict[str, Any]], Optional[DataFrame], Dict[str, Any]]:
    predictions_df = exporter.table(
        ML_PREDICTIONS,
        ["user_key", "topic_key", "session_id", "prediction", "struggle_probability", "model_version", "prediction_created_at"],
    )
    if predictions_df is None:
        return [], None, {"available": False, "reason": exporter.unavailable.get(ML_PREDICTIONS)}

    latest_df, metadata = latest_predictions(predictions_df)
    features_df = exporter.table(
        ML_FEATURES,
        [
            "user_key",
            "topic_key",
            "session_id",
            "failure_rate_last_7_days",
            "still_confused_rate",
            "illusion_gap_score",
            "repeated_mistake_count",
            "hints_used_last_7_days",
        ],
    )
    if features_df is not None:
        features_single_df = dedupe_features(features_df)
        joined_df = latest_df.alias("p").join(
            features_single_df.alias("f"),
            ["user_key", "topic_key", "session_id"],
            "left",
        )
        metadata["feature_source_rows"] = features_df.count()
        metadata["feature_deduplicated_rows"] = features_single_df.count()
    else:
        joined_df = latest_df
        for missing_signal_column in [
            "failure_rate_last_7_days",
            "still_confused_rate",
            "illusion_gap_score",
            "repeated_mistake_count",
            "hints_used_last_7_days",
        ]:
            joined_df = joined_df.withColumn(missing_signal_column, F.lit(None))
        metadata["feature_source_rows"] = None
        metadata["feature_deduplicated_rows"] = None

    signal_columns = [
        F.when(F.col("failure_rate_last_7_days").isNotNull(), F.concat(F.lit("Failure rate "), F.round(F.col("failure_rate_last_7_days"), 2).cast("string"))),
        F.when(F.col("still_confused_rate").isNotNull(), F.concat(F.lit("Confusion rate "), F.round(F.col("still_confused_rate"), 2).cast("string"))),
        F.when(F.col("illusion_gap_score").isNotNull(), F.concat(F.lit("Gap "), F.round(F.col("illusion_gap_score"), 2).cast("string"))),
        F.when(F.col("repeated_mistake_count") > 0, F.concat(F.col("repeated_mistake_count").cast("string"), F.lit(" repeated mistakes"))),
        F.when(F.col("hints_used_last_7_days") > 0, F.concat(F.col("hints_used_last_7_days").cast("string"), F.lit(" hints used"))),
    ]
    risk_df = (
        joined_df
        .withColumn("risk_score", F.col("struggle_probability").cast("float"))
        .withColumn("risk_level", risk_level(F.col("risk_score")))
        .withColumn("signal_array", F.array(*signal_columns))
        .withColumn("signal_array", F.expr("filter(signal_array, x -> x is not null)"))
        .withColumn("main_signal", F.coalesce(F.element_at("signal_array", 1), F.lit("Model probability")))
        .select(
            F.concat(F.lit("Learner "), F.col("user_key").cast("string")).alias("learner_id"),
            F.col("risk_score"),
            F.col("risk_level"),
            F.col("main_signal"),
            F.col("model_version"),
            F.col("prediction_created_at"),
        )
        .orderBy(F.col("risk_score").desc_nulls_last(), F.col("learner_id"))
    )
    return rows_as_dicts(risk_df, TOP_N), latest_df, metadata


def collect_statuses(df: Optional[DataFrame]) -> List[str]:
    if df is None:
        return []
    rows = df.select("quarantine_status").where(F.col("quarantine_status").isNotNull()).distinct().collect()
    return sorted([row["quarantine_status"] for row in rows])


def build_quality_summary(exporter: DashboardExporter) -> Dict[str, Any]:
    quality_tables = [
        exporter.table(BRONZE_QUALITY_RESULTS, ["status", "check_time", "source_table", "rule_name", "failed_rows"]),
        exporter.table(SILVER_QUALITY_RESULTS, ["status", "check_time", "source_table", "rule_name", "failed_rows"]),
    ]
    quarantine_tables = [
        exporter.table(BRONZE_QUARANTINE, ["quarantine_id", "quarantine_status", "detected_at"]),
        exporter.table(SILVER_QUARANTINE, ["quarantine_id", "quarantine_status", "detected_at"]),
    ]

    pass_count = warning_count = fail_count = 0
    for df in quality_tables:
        if df is None:
            continue
        pass_count += int(first_value(df.filter(F.upper(F.col("status")) == "PASS"), F.count("*"), "value") or 0)
        warning_count += int(first_value(df.filter(F.upper(F.col("status")) == "WARNING"), F.count("*"), "value") or 0)
        fail_count += int(first_value(df.filter(F.upper(F.col("status")) == "FAIL"), F.count("*"), "value") or 0)

    actual_statuses = sorted(set(status for df in quarantine_tables for status in collect_statuses(df)))
    normalized_statuses = {status.upper() for status in actual_statuses}
    recognizable_open_statuses = sorted(normalized_statuses.intersection(OPEN_STATUS_VALUES))

    quarantined_rows = 0
    quarantine_label = "total_quarantined_rows"
    quarantine_logic = "No quarantine_status values were found."
    if actual_statuses and recognizable_open_statuses:
        quarantine_label = "unresolved_quarantined_rows"
        quarantine_logic = f"Counted statuses matching unresolved/open values: {', '.join(recognizable_open_statuses)}."
        for df in quarantine_tables:
            if df is not None:
                quarantined_rows += int(first_value(df.filter(F.upper(F.col("quarantine_status")).isin(*recognizable_open_statuses)), F.count("*"), "value") or 0)
    else:
        quarantine_logic = (
            "Stored quarantine_status values do not clearly distinguish unresolved rows; "
            "reported total quarantined rows instead."
            if actual_statuses
            else quarantine_logic
        )
        for df in quarantine_tables:
            if df is not None:
                quarantined_rows += int(first_value(df, F.count("*"), "value") or 0)

    return {
        "pass_count": pass_count,
        "warning_count": warning_count,
        "fail_count": fail_count,
        "quarantined_rows": quarantined_rows,
        "quarantine_count_label": quarantine_label,
        "quarantine_status_values": actual_statuses,
        "quarantine_logic": quarantine_logic,
    }


def pipeline_status(exporter: DashboardExporter, generated_at: str, ml_metadata: Dict[str, Any]) -> List[Dict[str, str]]:
    gold_tables = [DIM_LEARNER, FACT_INTERACTION, FACT_ATTEMPT, FACT_CONCEPT_STATE, AGG_PROGRESS, AGG_ILLUSION]
    ml_tables = [ML_PREDICTIONS, ML_FEATURES]
    quality_tables = [BRONZE_QUALITY_RESULTS, SILVER_QUALITY_RESULTS, BRONZE_QUARANTINE, SILVER_QUARANTINE]

    def availability(component: str, table_names: Iterable[str], details: str) -> Dict[str, str]:
        missing = [table for table in table_names if table not in exporter.tables_read]
        if missing:
            return {
                "component": component,
                "status": "Partial",
                "details": f"{details} Unavailable: {', '.join(missing)}.",
            }
        return {"component": component, "status": "Available", "details": details}

    return [
        availability(
            "Gold metrics",
            gold_tables,
            "Gold tables were readable. This indicates availability, not that Gold quality passed.",
        ),
        availability(
            "ML output",
            ml_tables,
            f"Latest prediction selection exported. {ml_metadata.get('fallback_note') or 'prediction_created_at was available for latest selection.'}",
        ),
        availability(
            "Quality results",
            quality_tables,
            "Bronze/Silver quality result and quarantine tables were readable. Gold quality results are not persisted by the current pipeline.",
        ),
        {
            "component": "Dashboard JSON",
            "status": "Available",
            "details": f"Generated at {generated_at}.",
        },
    ]


def write_json_atomically(payload: Dict[str, Any], output_path: str) -> None:
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    temp_path = f"{output_path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(json_safe(payload), handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")
    os.replace(temp_path, output_path)


def log_catalog_configuration(spark: SparkSession) -> None:
    keys = [
        "spark.sql.catalog.demo",
        "spark.sql.catalog.demo.type",
        "spark.sql.catalog.demo.uri",
        "spark.sql.catalog.demo.warehouse",
    ]
    for key in keys:
        try:
            logger.info("Spark config | %s=%s", key, spark.conf.get(key))
        except Exception:
            logger.info("Spark config | %s=<not set in SparkConf>", key)


def run_job(spark: SparkSession) -> int:
    logger.info("export_dashboard_data_job started.")
    log_catalog_configuration(spark)
    exporter = DashboardExporter(spark)
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    learner_risk, latest_prediction_df, ml_metadata = build_learner_risk(exporter, None)
    payload = {
        "generated_at": generated_at,
        "status": "success",
        "kpis": build_kpis(exporter, latest_prediction_df),
        "weakest_concepts": build_weakest_concepts(exporter),
        "learner_risk": learner_risk,
        "learning_progress": build_learning_progress(exporter),
        "learning_gap": build_learning_gap(exporter),
        "quality_summary": build_quality_summary(exporter),
        "pipeline_status": [],
        "metadata": {
            "tables_read": sorted(exporter.tables_read),
            "unavailable_tables": exporter.unavailable,
            "ml_prediction_selection": ml_metadata,
        },
    }
    payload["pipeline_status"] = pipeline_status(exporter, generated_at, ml_metadata)
    if exporter.unavailable:
        payload["status"] = "partial"

    write_json_atomically(payload, OUTPUT_PATH)
    logger.info("Dashboard JSON exported | output_path=%s", OUTPUT_PATH)
    logger.info("Tables read | tables=%s", sorted(exporter.tables_read))
    logger.info("Metrics exported | sections=%s", [key for key in payload.keys() if key not in {"metadata"}])
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("export_dashboard_data_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
