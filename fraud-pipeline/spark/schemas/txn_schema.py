from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    BooleanType, IntegerType
)

TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_category", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", FloatType(), False),
    StructField("currency", StringType(), False),
    StructField("country", StringType(), False),
    StructField("city", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("ip_address", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("is_international", BooleanType(), False),
    StructField("is_velocity_burst", BooleanType(), False),
    StructField("is_high_risk_merchant", BooleanType(), False),
    StructField("baseline_risk_score", FloatType(), False),
    StructField("fraud_label", IntegerType(), False),
])