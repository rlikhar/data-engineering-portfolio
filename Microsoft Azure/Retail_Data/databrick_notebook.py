## Spark Configuration

spark.conf.set(
  "fs.azure.account.key.projectstorageretaildata.blob.core.windows.net",
  "ACCESS TOKEN"  --Replace with your Access Token
)

display(
  dbutils.fs.ls("wasbs://retailproject@projectstorageretaildata.blob.core.windows.net/bronze/transaction/")
)

## Read raw data from Bronze layer

df_transactions = spark.read.parquet(
    "wasbs://retailproject@projectstorageretaildata.blob.core.windows.net/bronze/transaction/"
)

df_products = spark.read.parquet(
    "wasbs://retailproject@projectstorageretaildata.blob.core.windows.net/bronze/product/"
)

df_stores = spark.read.parquet(
    "wasbs://retailproject@projectstorageretaildata.blob.core.windows.net/bronze/store/"
)

# DBTITLE 1,create silver layer - data cleaning
from pyspark.sql.functions import col

# Convert types and clean data
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


df_silver = df_transactions \
    .join(df_customers, "customer_id") \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))

display(df_silver)

# Write to Silver layer

silver_path = "wasbs://retailproject@projectstorageretaildata.blob.core.windows.net/silver/"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

df_silver.write \
    .mode("overwrite") \
    .saveAsTable("adf_olist_rs.default.retail_silver_cleaned")

## Create a Gold layer Transformation

%sql
SELECT * 
FROM adf_olist_rs.default.retail_silver_cleaned;

%sql
CREATE OR REPLACE TABLE adf_olist_rs.default.retail_gold_sales AS
SELECT
    transaction_date,
    product_id,
    product_name,
    category,
    store_id,
    store_name,
    location,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_amount) AS total_sales_amount,
    COUNT(DISTINCT transaction_id) AS number_of_transactions,
    AVG(total_amount) AS average_transaction_value
FROM adf_olist_rs.default.retail_silver_cleaned
GROUP BY
    transaction_date,
    product_id,
    product_name,
    category,
    store_id,
    store_name,
    location;


gold_df = spark.sql("""
SELECT
    transaction_date,
    product_id,
    product_name,
    category,
    store_id,
    store_name,
    location,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_amount) AS total_sales_amount,
    COUNT(DISTINCT transaction_id) AS number_of_transactions,
    AVG(total_amount) AS average_transaction_value
FROM adf_olist_rs.default.retail_silver_cleaned
GROUP BY
    transaction_date,
    product_id,
    product_name,
    category,
    store_id,
    store_name,
    location
""")

gold_df.write \
    .mode("overwrite") \
    .saveAsTable("adf_olist_rs.default.retail_gold_sales")
