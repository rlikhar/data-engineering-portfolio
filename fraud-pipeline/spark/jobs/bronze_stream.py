from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp
)

from spark.schemas.txn_schema import TXN_SCHEMA


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "txn-raw"
)

BASE_DIR = os.path.abspath(os.getcwd())

BRONZE_PATH = os.path.join(BASE_DIR, "delta/bronze")

CHECKPOINT_PATH = os.path.join(
    BASE_DIR,
    "spark/checkpoints/bronze"
)

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

DELTA_PACKAGE = (
    "io.delta:delta-spark_2.12:3.2.0"
)


def create_spark_session() -> SparkSession:

    builder = (
        SparkSession.builder
        .appName("fraud-bronze-stream")
        .master(SPARK_MASTER)

        .config(
            "spark.jars.packages",
            f"{KAFKA_PACKAGE},{DELTA_PACKAGE}"
        )

        .config("spark.sql.shuffle.partitions", "4")

        .config(
            "spark.sql.streaming.schemaInference",
            "false"
        )

        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )

        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():

    spark = create_spark_session()

    print("=" * 60)
    print("Starting Bronze Kafka Stream")
    print(f"Kafka Server : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print(f"Bronze Path  : {BRONZE_PATH}")
    print("=" * 60)

    # Read Kafka Stream
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON
    bronze_df = (
        kafka_df

        .selectExpr(
            "CAST(value AS STRING) AS json_str",
            "timestamp AS kafka_timestamp"
        )

        .select(
            from_json(
                col("json_str"),
                TXN_SCHEMA
            ).alias("txn"),

            col("json_str"),
            col("kafka_timestamp")
        )

        .select(
            "txn.*",
            "json_str",
            "kafka_timestamp"
        )

        .withColumn(
            "event_time_ts",
            to_timestamp(col("event_time"))
        )

        .withColumn(
            "ingest_time",
            current_timestamp()
        )

        .withColumn(
            "bronze_date",
            col("event_time_ts").cast("date")
        )
    )

    # Console Debug Sink
    console_query = (
        bronze_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    # Delta Sink
    delta_query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")

        .option(
            "checkpointLocation",
            CHECKPOINT_PATH
        )

        .partitionBy("bronze_date")

        .start(BRONZE_PATH)
    )

    print("Bronze stream started successfully.")
    print("Waiting for Kafka events...")

    delta_query.awaitTermination()


if __name__ == "__main__":
    main()