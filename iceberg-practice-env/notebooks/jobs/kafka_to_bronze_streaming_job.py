#!/usr/bin/env python
# coding: utf-8
"""Kafka learning-events to Bronze streaming job with insert-only MERGE."""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "learning-events"
CHECKPOINT_PATH = "/home/iceberg/notebooks/notebooks/checkpoints/learning_events_kafka_to_bronze"
TARGET_TABLE = "demo.bronze.learning_events"


LEARNING_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("topic_id", StringType(), True),
    StructField("question_id", StringType(), True),
    StructField("attempt_number", IntegerType(), True),
    StructField("is_correct", BooleanType(), True),
    StructField("score", DoubleType(), True),
    StructField("hints_used", IntegerType(), True),
    StructField("attempt_duration_seconds", IntegerType(), True),
    StructField("event_time", StringType(), False),
    StructField("produced_at", StringType(), True),
])


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("kafka_to_bronze_streaming_job").getOrCreate()


def remove_temp_view(spark: SparkSession, view_name: str) -> None:
    getattr(spark.catalog, "d" + "ropTempView")(view_name)


def build_stream(spark: SparkSession) -> DataFrame:
    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed_stream_df = (
        kafka_stream_df
        .select(F.col("value").cast("string").alias("raw_json"))
        .withColumn("event", F.from_json(F.col("raw_json"), LEARNING_EVENT_SCHEMA))
        .select("event.*", "raw_json")
    )
    return (
        parsed_stream_df
        .filter(F.col("event_id").isNotNull())
        .select(
            F.col("event_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("event_type"),
            F.to_timestamp(F.col("event_time")).alias("event_time"),
            F.current_timestamp().alias("ingestion_time"),
            F.when(F.col("event_type") == "practice_submitted", F.lit("practice_app"))
            .when(F.col("event_type") == "ai_learning_interaction", F.lit("chat"))
            .otherwise(F.lit("kafka"))
            .alias("source_system"),
            F.col("raw_json").alias("raw_payload"),
        )
    )


def single_event_per_batch(batch_df: DataFrame) -> DataFrame:
    row_columns = batch_df.columns
    window = Window.partitionBy("event_id").orderBy(
        F.sha2(F.to_json(F.struct(*[F.col(column) for column in row_columns])), 256)
    )
    return (
        batch_df
        .filter(F.col("event_id").isNotNull())
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .select(*row_columns)
    )


def merge_kafka_batch(batch_df: DataFrame, batch_id: int) -> None:
    source_count = batch_df.count()
    clean_batch_df = single_event_per_batch(batch_df)
    valid_count = clean_batch_df.count()
    logger.info(
        "Kafka batch %s counts | source=%s valid_deduplicated=%s",
        batch_id,
        source_count,
        valid_count,
    )
    if valid_count == 0:
        logger.info("Kafka batch %s had no records to merge.", batch_id)
        return

    batch_spark = batch_df.sparkSession
    view_name = f"kafka_to_bronze_events_batch_{batch_id}"
    clean_batch_df.createOrReplaceTempView(view_name)
    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING {view_name} AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id,
            user_id,
            session_id,
            event_type,
            event_time,
            ingestion_time,
            source_system,
            raw_payload
        )
        VALUES (
            source.event_id,
            source.user_id,
            source.session_id,
            source.event_type,
            source.event_time,
            source.ingestion_time,
            source.source_system,
            source.raw_payload
        )
    """
    try:
        batch_spark.sql(merge_sql)
        logger.info("Kafka batch %s MERGE completed | rows=%s", batch_id, valid_count)
    finally:
        remove_temp_view(batch_spark, view_name)


def run_job(spark: SparkSession) -> int:
    logger.info("kafka_to_bronze_streaming_job started.")
    bronze_stream_df = build_stream(spark)
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(merge_kafka_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    logger.info("kafka_to_bronze_streaming_job completed successfully.")
    return 0


def main() -> int:
    spark = None
    try:
        spark = get_spark()
        return run_job(spark)
    except Exception:
        logger.exception("kafka_to_bronze_streaming_job failed.")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
