#!/usr/bin/env python3
import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, TimestampType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("bronze_ingestion")

KAFKA_BROKERS          = os.getenv("KAFKA_BROKERS",          "kafka:9092")
MINIO_ENDPOINT         = os.getenv("MINIO_ENDPOINT",         "http://minio:9000")
MINIO_ACCESS_KEY       = os.getenv("MINIO_ACCESS_KEY",       "minioadmin")
MINIO_SECRET_KEY       = os.getenv("MINIO_SECRET_KEY",       "hshsh83skskk8lsk8320ljsks73")
BRONZE_BUCKET          = os.getenv("BRONZE_BUCKET",          "lakehouse-bronze")
CHECKPOINT_BASE        = os.getenv("CHECKPOINT_BASE",        f"s3a://{BRONZE_BUCKET}/_checkpoints/bronze")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
NESSIE_URI             = os.getenv("NESSIE_URI",             "http://nessie:19120/api/v1")

builder = SparkSession.builder \
    .appName("BronzeIngestion") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.iceberg.uri", NESSIE_URI) \
    .config("spark.sql.catalog.iceberg.ref", "main") \
    .config("spark.sql.catalog.iceberg.warehouse", f"s3a://{BRONZE_BUCKET}/warehouse") \
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
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.executorEnv.AWS_REGION", "us-east-1")

log.info("Spark session initialized")
log.info(f"Nessie URI: {NESSIE_URI}")

# ── Schémas réels extraits des messages Kafka/Debezium ────────────────────────
# Timestamps sont des strings ISO8601 → StringType puis cast en TimestampType
TABLE_SCHEMAS = {
    "customers": StructType([
        StructField("customer_id",       IntegerType(), True),
        StructField("full_name",         StringType(),  True),
        StructField("city",              StringType(),  True),
        StructField("segment",           StringType(),  True),
        StructField("registration_date", StringType(),  True),
        StructField("updated_at",        StringType(),  True),
    ]),
    "products": StructType([
        StructField("product_id",  IntegerType(), True),
        StructField("category",    StringType(),  True),
        StructField("brand",       StringType(),  True),
        StructField("unit_price",  StringType(),  True),  # encodé en base64 par Debezium
        StructField("stock_qty",   IntegerType(), True),
        StructField("created_at",  StringType(),  True),
        StructField("updated_at",  StringType(),  True),
    ]),
    "orders": StructType([
        StructField("order_id",    IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date",  StringType(),  True),
        StructField("status",      StringType(),  True),
        StructField("channel",     StringType(),  True),
        StructField("updated_at",  StringType(),  True),
    ]),
    "order_items": StructType([
        StructField("order_item_id", IntegerType(), True),
        StructField("order_id",      IntegerType(), True),
        StructField("product_id",    IntegerType(), True),
        StructField("quantity",      IntegerType(), True),
        StructField("unit_price",    StringType(),  True),
        StructField("total_amount",  StringType(),  True),
    ]),
    "payments": StructType([
        StructField("payment_id",     IntegerType(), True),
        StructField("order_id",       IntegerType(), True),
        StructField("payment_date",   StringType(),  True),
        StructField("payment_method", StringType(),  True),
        StructField("amount",         StringType(),  True),
        StructField("payment_status", StringType(),  True),
    ]),
    "returns": StructType([
        StructField("return_id",   IntegerType(), True),
        StructField("order_id",    IntegerType(), True),
        StructField("return_date", StringType(),  True),
        StructField("reason",      StringType(),  True),
        StructField("status",      StringType(),  True),
    ]),
}

# ── DDL Iceberg aligné sur les vrais schémas ─────────────────────────────────
TABLE_DDL = {
    "customers": """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.customers (
            op STRING, topic STRING, partition INT, offset BIGINT, kafka_timestamp TIMESTAMP,
            customer_id INT, full_name STRING, city STRING, segment STRING,
            registration_date STRING, updated_at STRING
        ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.products (
            op STRING, topic STRING, partition INT, offset BIGINT, kafka_timestamp TIMESTAMP,
            product_id INT, category STRING, brand STRING, unit_price STRING,
            stock_qty INT, created_at STRING, updated_at STRING
        ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.orders (
            op STRING, topic STRING, partition INT, offset BIGINT, kafka_timestamp TIMESTAMP,
            order_id INT, customer_id INT, order_date STRING, status STRING,
            channel STRING, updated_at STRING
        ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.order_items (
            op STRING, topic STRING, partition INT, offset BIGINT, kafka_timestamp TIMESTAMP,
            order_item_id INT, order_id INT, product_id INT, quantity INT,
            unit_price STRING, total_amount STRING
        ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')
    """,
    "payments": """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.payments (
            op STRING, topic STRING, partition INT, offset BIGINT, kafka_timestamp TIMESTAMP,
            payment_id INT, order_id INT, payment_date STRING, payment_method STRING,
            amount STRING, payment_status STRING
        ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')
    """,
    "returns": """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.returns (
            op STRING, topic STRING, partition INT, offset BIGINT, kafka_timestamp TIMESTAMP,
            return_id INT, order_id INT, return_date STRING, reason STRING, status STRING
        ) USING iceberg TBLPROPERTIES ('write.format.default'='parquet')
    """,
}

# Colonne PK par table (pour filtrer les lignes vides)
TABLE_PK = {
    "customers":  "customer_id",
    "products":   "product_id",
    "orders":     "order_id",
    "order_items":"order_item_id",
    "payments":   "payment_id",
    "returns":    "return_id",
}


def create_tables():
    log.info("=== Creating namespace iceberg.bronze ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    for table_name, ddl in TABLE_DDL.items():
        try:
            spark.sql(ddl)
            log.info(f"  OK iceberg.bronze.{table_name}")
        except Exception as e:
            log.error(f"  FAIL iceberg.bronze.{table_name}: {e}")
            raise

    log.info("=== All tables created ===")
    spark.sql("SHOW TABLES IN iceberg.bronze").show()


def process_table(table_name, schema):
    kafka_topic     = f"postgres.public.{table_name}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{table_name}"
    table_path      = f"iceberg.bronze.{table_name}"
    pk              = TABLE_PK[table_name]

    log.info(f"Stream: {kafka_topic} -> {table_path}")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = df.select(
        col("value").cast("string").alias("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).filter(col("value").isNotNull())

    parsed = parsed.select(
        get_json_object(col("value"), "$.op").alias("op"),
        get_json_object(col("value"), "$.after").alias("after"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
    ).filter(col("op").isin("c", "u", "r"))

    data_struct = parsed.withColumn("row", from_json(col("after"), schema))

    field_names = [f.name for f in schema.fields]
    final = data_struct.select(
        "op", "topic", "partition", "offset", "kafka_timestamp",
        *[col(f"row.{f}").alias(f) for f in field_names],
    ).filter(col(pk).isNotNull())

    return final.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="10 seconds") \
        .start(table_path)


def main():
    log.info("=== Bronze Ingestion starting ===")

    try:
        create_tables()
    except Exception as e:
        log.error(f"Table creation failed: {e}")
        sys.exit(1)

    queries = []
    for table_name, schema in TABLE_SCHEMAS.items():
        try:
            queries.append(process_table(table_name, schema))
            log.info(f"Stream started: {table_name}")
        except Exception as e:
            log.error(f"Stream failed: {table_name}: {e}")

    if not queries:
        log.error("No streams started. Exiting.")
        sys.exit(1)

    log.info(f"=== {len(queries)}/6 streams running ===")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()