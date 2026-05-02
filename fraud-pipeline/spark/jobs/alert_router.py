from __future__ import annotations

import json
import os
import urllib.request
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    concat_ws,
    to_json,
    struct,
    lit,
)

SILVER_PATH = os.path.join(os.path.abspath(os.getcwd()), "delta/silver")
ALERTS_AUDIT_PATH = os.path.join(os.path.abspath(os.getcwd()), "delta/alerts")
CHECKPOINT_PATH = os.path.join(os.path.abspath(os.getcwd()), "spark/checkpoints/alerts")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("fraud-alert-router")
        .master("local[*]")
        .config("spark.jars.packages", f"{KAFKA_PACKAGE},{DELTA_PACKAGE}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _send_slack_message(text: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return

    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def _build_alerts(df):
    return (
        df.filter(
            (col("fraud_rule_triggered") == True) |
            (col("high_amount_flag") == True) |
            (col("high_customer_risk_flag") == True)
        )
        .withColumn("alert_id", expr("uuid()"))
        .withColumn("alert_time", current_timestamp())
        .withColumn(
            "alert_reason",
            expr("""
                CASE
                    WHEN amount >= 10000 THEN 'EXTREME_AMOUNT'
                    WHEN amount > 8000 THEN 'HIGH_AMOUNT'
                    WHEN customer_risk_score > 0.7 THEN 'HIGH_CUSTOMER_RISK'
                    WHEN previous_fraud_count >= 2 THEN 'REPEAT_FRAUD_HISTORY'
                    WHEN is_velocity_burst = true THEN 'VELOCITY_BURST'
                    ELSE 'RULE_TRIGGERED'
                END
            """)
        )
        .withColumn(
            "severity",
            expr("""
                CASE
                    WHEN amount >= 10000 OR customer_risk_score >= 0.9 THEN 'CRITICAL'
                    WHEN amount > 8000 OR customer_risk_score > 0.7 THEN 'HIGH'
                    ELSE 'MEDIUM'
                END
            """)
        )
        .withColumn("alert_key", concat_ws(":", col("customer_id"), col("transaction_id")))
    )


def process_batch(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    alerts_df = _build_alerts(batch_df)

    if alerts_df.rdd.isEmpty():
        return

    kafka_payload_df = alerts_df.selectExpr(
        "CAST(alert_key AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    kafka_payload_df.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", ALERTS_TOPIC) \
        .save()

    alerts_df.write.format("delta").mode("append").save(ALERTS_AUDIT_PATH)

    alert_rows = alerts_df.select(
        "customer_id",
        "amount",
        "customer_risk_score",
        "alert_reason",
        "severity"
    ).limit(5).collect()

    if alert_rows:
        lines = [
            f"- {r['customer_id']} | {r['amount']} | {r['customer_risk_score']} | {r['alert_reason']} | {r['severity']}"
            for r in alert_rows
        ]
        message = f"Fraud alerts batch {batch_id}: {len(alert_rows)} alert(s)\n" + "\n".join(lines)
        _send_slack_message(message)

    print(f"Alert batch {batch_id} routed with {alerts_df.count()} alert(s).")


def main() -> None:
    spark = create_spark_session()

    silver_df = spark.readStream.format("delta").load(SILVER_PATH)

    query = (
        silver_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print("=" * 60)
    print("Alert routing stream started")
    print("=" * 60)

    query.awaitTermination()


if __name__ == "__main__":
    main()