#!/usr/bin/env python
# coding: utf-8
"""Validation-only job for late-arriving Bronze feedback."""

from __future__ import annotations

import logging
import sys
from functools import reduce
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

BRONZE_FEEDBACK = "demo.bronze.learning_feedback"
SILVER_FEEDBACK_BY_STAGE: Dict[str, str] = {
    "before_practice": "demo.silver.pre_practice_feedback",
    "after_practice": "demo.silver.post_practice_feedback",
    "general_check_in": "demo.silver.learner_check_in",
}

STATUS_INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
STATUS_INVALID_NEGATIVE_DELAY = "INVALID_NEGATIVE_DELAY"
STATUS_ACCEPTED = "ACCEPTED"
STATUS_TOO_LATE = "TOO_LATE"


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("late_arriving_feedback_job").getOrCreate()


def classify_feedback(bronze_feedback_df: DataFrame) -> DataFrame:
    return (
        bronze_feedback_df
        .withColumn(
            "delay_minutes_calculated",
            (F.col("ingestion_time").cast("long") - F.col("feedback_time").cast("long")) / 60.0,
        )
        .withColumn("delay_hours_calculated", F.col("delay_minutes_calculated") / 60.0)
        .withColumn(
            "late_arrival_status",
            F.when(
                F.col("feedback_time").isNull() | F.col("ingestion_time").isNull(),
                F.lit(STATUS_INVALID_TIMESTAMP),
            )
            .when(F.col("delay_minutes_calculated") < 0, F.lit(STATUS_INVALID_NEGATIVE_DELAY))
            .when(F.col("delay_hours_calculated") <= 48, F.lit(STATUS_ACCEPTED))
            .otherwise(F.lit(STATUS_TOO_LATE)),
        )
    )


def log_status_counts(classified_df: DataFrame) -> Dict[str, int]:
    counts = {
        row["late_arrival_status"]: int(row["row_count"])
        for row in (
            classified_df
            .groupBy("late_arrival_status")
            .agg(F.count("*").alias("row_count"))
            .collect()
        )
    }
    for status in [
        STATUS_ACCEPTED,
        STATUS_TOO_LATE,
        STATUS_INVALID_TIMESTAMP,
        STATUS_INVALID_NEGATIVE_DELAY,
    ]:
        logger.info("Late-arrival status count | status=%s rows=%s", status, counts.get(status, 0))
    return counts


def union_all(dataframes: List[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), dataframes)


def feedback_ids(df: DataFrame) -> List[str]:
    return [
        row["feedback_id"]
        for row in (
            df.select("feedback_id")
            .where(F.col("feedback_id").isNotNull())
            .distinct()
            .orderBy("feedback_id")
            .collect()
        )
    ]


def expected_silver_violations(classified_df: DataFrame, status: str, require_present: bool) -> DataFrame:
    checks = []
    for feedback_stage, silver_table in SILVER_FEEDBACK_BY_STAGE.items():
        bronze_subset_df = (
            classified_df
            .filter((F.col("late_arrival_status") == status) & (F.col("feedback_stage") == feedback_stage))
            .select("feedback_id", "feedback_stage", "late_arrival_status")
        )
        silver_ids_df = (
            classified_df.sparkSession
            .table(silver_table)
            .select(F.col("feedback_id").alias("silver_feedback_id"))
        )
        if require_present:
            violation_df = (
                bronze_subset_df.alias("b")
                .join(
                    silver_ids_df.alias("s"),
                    F.col("b.feedback_id") == F.col("s.silver_feedback_id"),
                    "left_anti",
                )
                .select("b.feedback_id", "b.feedback_stage", "b.late_arrival_status")
            )
        else:
            violation_df = (
                bronze_subset_df.alias("b")
                .join(
                    silver_ids_df.alias("s"),
                    F.col("b.feedback_id") == F.col("s.silver_feedback_id"),
                    "inner",
                )
                .select("b.feedback_id", "b.feedback_stage", "b.late_arrival_status")
            )
        checks += [violation_df]
    return union_all(checks)


def run_job(spark: SparkSession) -> int:
    logger.info("late_arriving_feedback_job started.")
    bronze_feedback_df = spark.table(BRONZE_FEEDBACK)
    source_count = bronze_feedback_df.count()
    logger.info("Bronze feedback source rows=%s", source_count)

    classified_df = classify_feedback(bronze_feedback_df)
    counts = log_status_counts(classified_df)
    total_classified = sum(counts.values())
    classification_complete = total_classified == source_count
    logger.info(
        "Classification completeness | source=%s classified=%s complete=%s",
        source_count,
        total_classified,
        classification_complete,
    )

    invalid_count = counts.get(STATUS_INVALID_TIMESTAMP, 0) + counts.get(STATUS_INVALID_NEGATIVE_DELAY, 0)
    accepted_missing_df = expected_silver_violations(
        classified_df,
        status=STATUS_ACCEPTED,
        require_present=True,
    )
    accepted_missing_count = accepted_missing_df.count()
    accepted_missing_ids = feedback_ids(accepted_missing_df)
    logger.info(
        "ACCEPTED feedback missing from expected Silver | rows=%s feedback_ids=%s",
        accepted_missing_count,
        accepted_missing_ids,
    )

    too_late_present_df = expected_silver_violations(
        classified_df,
        status=STATUS_TOO_LATE,
        require_present=False,
    )
    too_late_present_count = too_late_present_df.count()
    too_late_present_ids = feedback_ids(too_late_present_df)
    logger.info(
        "TOO_LATE feedback incorrectly present in Silver | rows=%s feedback_ids=%s",
        too_late_present_count,
        too_late_present_ids,
    )

    if (
        not classification_complete
        or invalid_count > 0
        or accepted_missing_count > 0
        or too_late_present_count > 0
    ):
        logger.error(
            "late_arriving_feedback_job failed validation | complete=%s invalid=%s accepted_missing=%s too_late_present=%s",
            classification_complete,
            invalid_count,
            accepted_missing_count,
            too_late_present_count,
        )
        return 1

    logger.info("late_arriving_feedback_job completed successfully.")
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("late_arriving_feedback_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
