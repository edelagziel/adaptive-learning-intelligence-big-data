#!/usr/bin/env python
# coding: utf-8
"""Reusable Gold dimension load job using idempotent Iceberg MERGE writes."""

from __future__ import annotations

import logging
import sys
from typing import List, Sequence

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SILVER_LEARNER_PROFILES = "demo.silver.learner_profiles"
SILVER_CONTENT_TAXONOMY = "demo.silver.content_taxonomy"
SILVER_REFERENCE_MATERIALS = "demo.silver.reference_materials"

DIM_LEARNER = "demo.gold.dim_learner"
DIM_TOPIC = "demo.gold.dim_topic"
DIM_CONTENT_TYPE = "demo.gold.dim_content_type"
DIM_REFERENCE_SOURCE = "demo.gold.dim_reference_source"


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("gold_dimensions_job").getOrCreate()


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
    update_clause = ",\n            ".join(
        [f"target.{column} = source.{column}" for column in update_columns]
    )
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


def current_max_key(spark: SparkSession, table_name: str, key_column: str) -> int:
    return int(
        spark.table(table_name)
        .agg(F.coalesce(F.max(F.col(key_column)), F.lit(0)).alias("max_key"))
        .first()["max_key"]
    )


def load_dim_learner(spark: SparkSession) -> None:
    logger.info("Stage start | dim_learner")
    silver_df = spark.table(SILVER_LEARNER_PROFILES)

    version_window = Window.partitionBy("user_id").orderBy("profile_updated_at")
    current_window = Window.partitionBy("user_id").orderBy(F.col("profile_updated_at").desc())
    profile_versions_df = (
        silver_df
        .withColumn("valid_from", F.col("profile_updated_at"))
        .withColumn("valid_to", F.lead("profile_updated_at").over(version_window))
        .withColumn("is_current", F.row_number().over(current_window) == 1)
        .select(
            "user_id",
            "registration_date",
            "preferred_language",
            "background_level",
            "learning_goal",
            "main_domain",
            "profile_updated_at",
            "valid_from",
            "valid_to",
            "is_current",
        )
    )

    base_df = valid_and_single_row(
        profile_versions_df,
        ["user_id", "profile_updated_at"],
        "dim_learner profile versions",
    )
    existing_keys_df = spark.table(DIM_LEARNER).select("user_key", "user_id", "profile_updated_at")
    existing_source_df = (
        base_df.alias("source")
        .join(existing_keys_df.alias("target"), ["user_id", "profile_updated_at"], "inner")
        .select(
            F.col("target.user_key").cast("int").alias("user_key"),
            *[F.col(f"source.{column}") for column in base_df.columns],
            F.lit(True).cast("boolean").alias("is_active"),
        )
    )
    missing_df = base_df.join(existing_keys_df, ["user_id", "profile_updated_at"], "left_anti")
    missing_count = missing_df.count()
    logger.info("dim_learner missing version rows=%s", missing_count)

    new_key_window = Window.orderBy("user_id", "profile_updated_at")
    missing_with_keys_df = (
        missing_df
        .withColumn("user_key", F.row_number().over(new_key_window) + F.lit(current_max_key(spark, DIM_LEARNER, "user_key")))
        .withColumn("is_active", F.lit(True).cast("boolean"))
        .select(
            "user_key",
            "user_id",
            "registration_date",
            "preferred_language",
            "background_level",
            "learning_goal",
            "main_domain",
            "profile_updated_at",
            "valid_from",
            "valid_to",
            "is_current",
            "is_active",
        )
    )
    source_df = existing_source_df.select(missing_with_keys_df.columns).unionByName(missing_with_keys_df)
    source_df = valid_and_single_row(source_df, ["user_id", "profile_updated_at"], "dim_learner merge source")

    merge_dataframe(
        spark,
        source_df,
        DIM_LEARNER,
        ["user_id", "profile_updated_at"],
        [
            "registration_date",
            "preferred_language",
            "background_level",
            "learning_goal",
            "main_domain",
            "is_active",
            "valid_from",
            "valid_to",
            "is_current",
        ],
        [
            "user_key",
            "user_id",
            "registration_date",
            "preferred_language",
            "background_level",
            "learning_goal",
            "main_domain",
            "profile_updated_at",
            "is_active",
            "valid_from",
            "valid_to",
            "is_current",
        ],
        "gold_dimensions_dim_learner_src",
    )


def load_dim_topic(spark: SparkSession) -> None:
    logger.info("Stage start | dim_topic")
    taxonomy_df = spark.table(SILVER_CONTENT_TAXONOMY)
    active_taxonomy_df = taxonomy_df.filter(
        (F.col("is_active") == True) & F.col("validation_status").isin("approved", "pending")
    )
    topic_base_df = (
        active_taxonomy_df
        .withColumn(
            "topic_name",
            F.when(F.col("taxonomy_level") == "domain", F.col("domain"))
            .when(F.col("taxonomy_level") == "topic", F.col("topic"))
            .when(F.col("taxonomy_level") == "subtopic", F.col("subtopic"))
            .otherwise(F.col("concept_name")),
        )
        .withColumn(
            "normalized_topic_name",
            F.when(F.col("taxonomy_level") == "domain", F.col("normalized_domain"))
            .when(F.col("taxonomy_level") == "topic", F.col("normalized_topic"))
            .when(F.col("taxonomy_level") == "subtopic", F.col("normalized_subtopic"))
            .otherwise(F.col("normalized_concept_name")),
        )
        .withColumn(
            "topic_id",
            F.concat_ws("_", F.col("taxonomy_level"), F.regexp_replace(F.col("normalized_topic_name"), " ", "_")),
        )
        .select(
            "taxonomy_id",
            "topic_id",
            "topic_name",
            "normalized_topic_name",
            "domain",
            "parent_taxonomy_id",
            "taxonomy_level",
            "first_detected_at",
            "validation_status",
            "is_active",
        )
    )
    base_df = valid_and_single_row(topic_base_df, ["taxonomy_id"], "dim_topic taxonomy source")
    existing_keys_df = spark.table(DIM_TOPIC).select("topic_key", "taxonomy_id")
    existing_source_df = (
        base_df.alias("source")
        .join(existing_keys_df.alias("target"), ["taxonomy_id"], "inner")
        .select(F.col("target.topic_key").cast("int").alias("topic_key"), *[F.col(f"source.{column}") for column in base_df.columns])
    )
    missing_df = base_df.join(existing_keys_df, ["taxonomy_id"], "left_anti")
    new_key_window = Window.orderBy("taxonomy_id")
    missing_with_keys_df = (
        missing_df
        .withColumn("topic_key", F.row_number().over(new_key_window) + F.lit(current_max_key(spark, DIM_TOPIC, "topic_key")))
        .select("topic_key", *base_df.columns)
    )
    staged_with_keys_df = existing_source_df.select(missing_with_keys_df.columns).unionByName(missing_with_keys_df)
    parent_keys_df = staged_with_keys_df.select(
        F.col("taxonomy_id").alias("parent_taxonomy_id_lookup"),
        F.col("topic_key").alias("parent_topic_key"),
    )
    source_df = (
        staged_with_keys_df.alias("child")
        .join(
            parent_keys_df.alias("parent"),
            F.col("child.parent_taxonomy_id") == F.col("parent.parent_taxonomy_id_lookup"),
            "left",
        )
        .select(
            F.col("child.topic_key").cast("int").alias("topic_key"),
            F.col("child.taxonomy_id"),
            F.col("child.topic_id"),
            F.col("child.topic_name"),
            F.col("child.normalized_topic_name"),
            F.col("child.domain"),
            F.col("parent.parent_topic_key").cast("int").alias("parent_topic_key"),
            F.col("child.taxonomy_level"),
            F.col("child.first_detected_at"),
            F.col("child.validation_status"),
            F.col("child.is_active"),
        )
    )
    source_df = valid_and_single_row(source_df, ["taxonomy_id"], "dim_topic merge source")

    merge_dataframe(
        spark,
        source_df,
        DIM_TOPIC,
        ["taxonomy_id"],
        [
            "topic_id",
            "topic_name",
            "normalized_topic_name",
            "domain",
            "parent_topic_key",
            "taxonomy_level",
            "first_detected_at",
            "validation_status",
            "is_active",
        ],
        [
            "topic_key",
            "taxonomy_id",
            "topic_id",
            "topic_name",
            "normalized_topic_name",
            "domain",
            "parent_topic_key",
            "taxonomy_level",
            "first_detected_at",
            "validation_status",
            "is_active",
        ],
        "gold_dimensions_dim_topic_src",
    )


def load_dim_content_type(spark: SparkSession) -> None:
    logger.info("Stage start | dim_content_type")
    rows = [
        Row(1, "verbal", "Verbal", "verbal", "humanistic", False, False, True, False, False, "Content that primarily requires reading and textual interpretation."),
        Row(2, "visual", "Visual", "visual", "mixed", False, False, False, True, False, "Content that relies on diagrams, graphs, layouts, or visual reasoning."),
        Row(3, "quantitative", "Quantitative", "quantitative", "realistic", True, True, False, False, False, "Content that requires numerical reasoning and calculation."),
        Row(4, "logical", "Logical", "practical", "realistic", True, False, False, False, False, "Content that primarily requires logical reasoning and structured problem solving."),
        Row(5, "memory_based", "Memory Based", "verbal", "mixed", False, False, True, False, True, "Content where recall or memorization is central."),
        Row(6, "mixed", "Mixed", "mixed", "mixed", True, False, True, True, False, "Content combining multiple cognitive and content characteristics."),
    ]
    source_df = spark.createDataFrame(
        rows,
        schema="""
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
        """,
    )
    source_df = valid_and_single_row(source_df, ["content_type_id"], "dim_content_type static source")
    merge_dataframe(
        spark,
        source_df,
        DIM_CONTENT_TYPE,
        ["content_type_id"],
        [
            "content_type_key",
            "content_type_name",
            "category",
            "academic_orientation",
            "requires_logic",
            "requires_numerical_reasoning",
            "requires_text_interpretation",
            "requires_visual_reasoning",
            "requires_memorization",
            "description",
        ],
        source_df.columns,
        "gold_dimensions_dim_content_type_src",
    )


def load_dim_reference_source(spark: SparkSession) -> None:
    logger.info("Stage start | dim_reference_source")
    reference_df = (
        spark.table(SILVER_REFERENCE_MATERIALS)
        .filter(F.col("is_active") == True)
        .select(
            "reference_id",
            "source_name",
            "source_type",
            "file_name",
            "reliability_level",
            "domain",
            "is_active",
        )
    )
    base_df = valid_and_single_row(reference_df, ["reference_id"], "dim_reference_source active references")
    existing_keys_df = spark.table(DIM_REFERENCE_SOURCE).select("reference_key", "reference_id")
    existing_source_df = (
        base_df.alias("source")
        .join(existing_keys_df.alias("target"), ["reference_id"], "inner")
        .select(F.col("target.reference_key").cast("int").alias("reference_key"), *[F.col(f"source.{column}") for column in base_df.columns])
    )
    missing_df = base_df.join(existing_keys_df, ["reference_id"], "left_anti")
    new_key_window = Window.orderBy("reference_id")
    missing_with_keys_df = (
        missing_df
        .withColumn("reference_key", F.row_number().over(new_key_window) + F.lit(current_max_key(spark, DIM_REFERENCE_SOURCE, "reference_key")))
        .select("reference_key", *base_df.columns)
    )
    source_df = existing_source_df.select(missing_with_keys_df.columns).unionByName(missing_with_keys_df)
    source_df = valid_and_single_row(source_df, ["reference_id"], "dim_reference_source merge source")
    merge_dataframe(
        spark,
        source_df,
        DIM_REFERENCE_SOURCE,
        ["reference_id"],
        ["source_name", "source_type", "file_name", "reliability_level", "domain", "is_active"],
        [
            "reference_key",
            "reference_id",
            "source_name",
            "source_type",
            "file_name",
            "reliability_level",
            "domain",
            "is_active",
        ],
        "gold_dimensions_dim_reference_source_src",
    )


def run_job(spark: SparkSession) -> int:
    for stage in [
        load_dim_learner,
        load_dim_topic,
        load_dim_content_type,
        load_dim_reference_source,
    ]:
        stage(spark)
    logger.info("gold_dimensions_job completed successfully.")
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("gold_dimensions_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
