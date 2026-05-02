from __future__ import annotations

import json
import os
from typing import Iterator, Dict, Any

import redis
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    BooleanType,
    IntegerType,
    TimestampType,
)

BASE_DIR = os.path.abspath(os.getcwd())

BRONZE_PATH = os.path.join(BASE_DIR, "delta/bronze")
SILVER_PATH = os.path.join(BASE_DIR, "delta/silver")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "spark/checkpoints/silver")

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("fraud-silver-stream")
        .master("local[*]")
        .config("spark.jars.packages", DELTA_PACKAGE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


spark = create_spark_session()


def enrich_partition(rows: Iterator[Row]) -> Iterator[Row]:
    """
    Create one Redis client per partition, not per row.
    This avoids Spark pickling errors from serializing the client.
    """
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    for row in rows:
        base = row.asDict(recursive=True)

        raw_profile = r.get(base["customer_id"])
        if raw_profile:
            profile = json.loads(raw_profile)
            customer_risk_score = float(profile.get("customer_risk_score", 0.0))
            kyc_verified = bool(profile.get("kyc_verified", False))
            account_age_days = int(profile.get("account_age_days", 0))
            previous_fraud_count = int(profile.get("previous_fraud_count", 0))
            avg_transaction_amount = float(profile.get("avg_transaction_amount", 0.0))
        else:
            customer_risk_score = 0.0
            kyc_verified = False
            account_age_days = 0
            previous_fraud_count = 0
            avg_transaction_amount = 0.0

        base["customer_risk_score"] = customer_risk_score
        base["kyc_verified"] = kyc_verified
        base["account_age_days"] = account_age_days
        base["previous_fraud_count"] = previous_fraud_count
        base["avg_transaction_amount"] = avg_transaction_amount

        yield Row(**base)


def process_batch(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    enriched_rdd = batch_df.rdd.mapPartitions(enrich_partition)
    enriched_df = spark.createDataFrame(enriched_rdd)

    silver_df = (
        enriched_df
        .withColumn("high_amount_flag", expr("CASE WHEN amount > 8000 THEN true ELSE false END"))
        .withColumn("high_customer_risk_flag", expr("CASE WHEN customer_risk_score > 0.7 THEN true ELSE false END"))
        .withColumn(
            "fraud_rule_triggered",
            expr("""
                CASE
                    WHEN amount > 8000
                      OR customer_risk_score > 0.7
                      OR previous_fraud_count >= 2
                      OR is_velocity_burst = true
                    THEN true
                    ELSE false
                END
            """)
        )
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_df.write.format("delta").mode("append").save(SILVER_PATH)

    print(f"Batch {batch_id} written to Silver. Rows: {silver_df.count()}")


def main() -> None:
    bronze_df = spark.readStream.format("delta").load(BRONZE_PATH)

    query = (
        bronze_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print("=" * 60)
    print("Silver enrichment stream started")
    print("=" * 60)

    query.awaitTermination()


if __name__ == "__main__":
    main()