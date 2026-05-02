from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as _sum,
    avg,
    max as _max,
    round as _round,
    when,
    to_date,
)

SILVER_PATH = os.path.join(os.path.abspath(os.getcwd()), "delta/silver")
GOLD_PATH = os.path.join(os.path.abspath(os.getcwd()), "delta/gold/customer_daily_summary")

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("fraud-gold-analytics")
        .master("local[*]")
        .config("spark.jars.packages", DELTA_PACKAGE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = create_spark_session()

    silver_df = spark.read.format("delta").load(SILVER_PATH)

    gold_df = (
        silver_df
        .withColumn("business_date", col("bronze_date"))
        .groupBy("business_date", "customer_id")
        .agg(
            count("*").alias("txn_count"),
            _sum(when(col("fraud_rule_triggered") == True, 1).otherwise(0)).alias("fraud_txn_count"),
            _round(avg("amount"), 2).alias("avg_amount"),
            _round(_sum("amount"), 2).alias("total_amount"),
            _round(avg("customer_risk_score"), 4).alias("avg_customer_risk_score"),
            _round(_max("customer_risk_score"), 4).alias("max_customer_risk_score"),
            _sum(when(col("high_amount_flag") == True, 1).otherwise(0)).alias("high_amount_txn_count"),
            _sum(when(col("high_customer_risk_flag") == True, 1).otherwise(0)).alias("high_risk_txn_count"),
        )
    )

    gold_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("business_date") \
        .save(GOLD_PATH)

    print("=" * 60)
    print("Gold analytics table written successfully")
    print("=" * 60)
    gold_df.show(20, truncate=False)


if __name__ == "__main__":
    main()