#!/usr/bin/env python3
import logging, os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, trim, lower

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("silver_processing")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "hshsh83skskk8lsk8320ljsks73")
SILVER_BUCKET    = os.getenv("SILVER_BUCKET",    "lakehouse-silver")
NESSIE_URI       = os.getenv("NESSIE_URI",       "http://nessie:19120/api/v1")
CHECKPOINT_BASE  = os.getenv("CHECKPOINT_BASE",  f"s3a://{SILVER_BUCKET}/_checkpoints/silver")

spark = SparkSession.builder \
    .appName("SilverProcessing") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.iceberg.uri", NESSIE_URI) \
    .config("spark.sql.catalog.iceberg.ref", "main") \
    .config("spark.sql.catalog.iceberg.warehouse", f"s3a://{SILVER_BUCKET}/warehouse") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT) \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", MINIO_ACCESS_KEY) \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_SECRET_KEY) \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.catalog.iceberg.s3.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.executorEnv.AWS_REGION", "us-east-1")
log.info("Silver Processing — Spark session initialized")

SILVER_DDL = {
    "customers": """CREATE TABLE IF NOT EXISTS iceberg.silver.customers (
        customer_id INT, full_name STRING, city STRING, segment STRING,
        registration_date TIMESTAMP, updated_at TIMESTAMP,
        _ingested_at TIMESTAMP, _op STRING
    ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')""",
    "products": """CREATE TABLE IF NOT EXISTS iceberg.silver.products (
        product_id INT, category STRING, brand STRING, stock_qty INT,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        _ingested_at TIMESTAMP, _op STRING
    ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')""",
    "orders": """CREATE TABLE IF NOT EXISTS iceberg.silver.orders (
        order_id INT, customer_id INT, order_date TIMESTAMP,
        status STRING, channel STRING, updated_at TIMESTAMP,
        _ingested_at TIMESTAMP, _op STRING
    ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')""",
    "order_items": """CREATE TABLE IF NOT EXISTS iceberg.silver.order_items (
        order_item_id INT, order_id INT, product_id INT, quantity INT,
        _ingested_at TIMESTAMP, _op STRING
    ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')""",
    "payments": """CREATE TABLE IF NOT EXISTS iceberg.silver.payments (
        payment_id INT, order_id INT, payment_date TIMESTAMP,
        payment_method STRING, payment_status STRING,
        _ingested_at TIMESTAMP, _op STRING
    ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')""",
    "returns": """CREATE TABLE IF NOT EXISTS iceberg.silver.returns (
        return_id INT, order_id INT, return_date TIMESTAMP,
        reason STRING, status STRING,
        _ingested_at TIMESTAMP, _op STRING
    ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')""",
}

def create_silver_tables():
    log.info("=== Creating namespace iceberg.silver ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    for t, ddl in SILVER_DDL.items():
        spark.sql(ddl)
        log.info(f"  OK iceberg.silver.{t}")
    spark.sql("SHOW TABLES IN iceberg.silver").show()

def transform_customers(df):
    return df.filter(col("customer_id").isNotNull()) \
        .filter(col("full_name").isNotNull()) \
        .dropDuplicates(["customer_id"]) \
        .withColumn("registration_date", to_timestamp(col("registration_date"))) \
        .withColumn("updated_at", to_timestamp(col("updated_at"))) \
        .withColumn("segment", lower(trim(col("segment")))) \
        .withColumn("full_name", trim(col("full_name"))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_op", col("op")) \
        .select("customer_id","full_name","city","segment","registration_date","updated_at","_ingested_at","_op")

def transform_products(df):
    return df.filter(col("product_id").isNotNull()) \
        .dropDuplicates(["product_id"]) \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .withColumn("updated_at", to_timestamp(col("updated_at"))) \
        .withColumn("category", trim(col("category"))) \
        .withColumn("brand", trim(col("brand"))) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_op", col("op")) \
        .select("product_id","category","brand","stock_qty","created_at","updated_at","_ingested_at","_op")

def transform_orders(df):
    return df.filter(col("order_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .dropDuplicates(["order_id"]) \
        .withColumn("order_date", to_timestamp(col("order_date"))) \
        .withColumn("updated_at", to_timestamp(col("updated_at"))) \
        .withColumn("status", lower(trim(col("status")))) \
        .withColumn("channel", lower(trim(col("channel")))) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_op", col("op")) \
        .select("order_id","customer_id","order_date","status","channel","updated_at","_ingested_at","_op")

def transform_order_items(df):
    return df.filter(col("order_item_id").isNotNull()) \
        .filter(col("quantity") > 0) \
        .dropDuplicates(["order_item_id"]) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_op", col("op")) \
        .select("order_item_id","order_id","product_id","quantity","_ingested_at","_op")

def transform_payments(df):
    return df.filter(col("payment_id").isNotNull()) \
        .filter(col("order_id").isNotNull()) \
        .dropDuplicates(["payment_id"]) \
        .withColumn("payment_date", to_timestamp(col("payment_date"))) \
        .withColumn("payment_method", lower(trim(col("payment_method")))) \
        .withColumn("payment_status", lower(trim(col("payment_status")))) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_op", col("op")) \
        .select("payment_id","order_id","payment_date","payment_method","payment_status","_ingested_at","_op")

def transform_returns(df):
    return df.filter(col("return_id").isNotNull()) \
        .filter(col("order_id").isNotNull()) \
        .dropDuplicates(["return_id"]) \
        .withColumn("return_date", to_timestamp(col("return_date"))) \
        .withColumn("reason", lower(trim(col("reason")))) \
        .withColumn("status", lower(trim(col("status")))) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_op", col("op")) \
        .select("return_id","order_id","return_date","reason","status","_ingested_at","_op")

TABLES = {
    "customers":   transform_customers,
    "products":    transform_products,
    "orders":      transform_orders,
    "order_items": transform_order_items,
    "payments":    transform_payments,
    "returns":     transform_returns,
}

def process_table(table_name, transform_fn):
    log.info(f"Processing: iceberg.bronze.{table_name} -> iceberg.silver.{table_name}")
    df = spark.readStream.format("iceberg").load(f"iceberg.bronze.{table_name}")
    transformed = transform_fn(df)
    return transformed.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table_name}") \
        .trigger(processingTime="30 seconds") \
        .start(f"iceberg.silver.{table_name}")

def main():
    log.info("=== Silver Processing starting ===")
    create_silver_tables()
    queries = []
    for table_name, fn in TABLES.items():
        try:
            queries.append(process_table(table_name, fn))
            log.info(f"Stream started: {table_name}")
        except Exception as e:
            log.error(f"Stream failed for {table_name}: {e}")
    if not queries:
        log.error("No streams started. Exiting.")
        sys.exit(1)
    log.info(f"=== {len(queries)}/6 silver streams running ===")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
