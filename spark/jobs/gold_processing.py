#!/usr/bin/env python3
"""
Gold Layer Processing — Batch Aggregations
Silver → Gold : KPIs métier, agrégations, joins
Tables: gold_sales_facts, gold_payments_summary, gold_returns_summary
+ gold_customer_segments, gold_daily_revenue, gold_product_performance
"""
import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, countDistinct,
    when, lit, round, current_timestamp, date_trunc,
    coalesce, datediff, to_date
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("gold_processing")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "hshsh83skskk8lsk8320ljsks73")
GOLD_BUCKET      = os.getenv("GOLD_BUCKET",       "lakehouse-gold")
NESSIE_URI       = os.getenv("NESSIE_URI",        "http://nessie:19120/api/v1")


spark = SparkSession.builder \
    .appName("GoldProcessing") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.iceberg.uri", NESSIE_URI) \
    .config("spark.sql.catalog.iceberg.ref", "main") \
    .config("spark.sql.catalog.iceberg.warehouse", f"s3a://{GOLD_BUCKET}/warehouse") \
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
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.executorEnv.AWS_REGION", "us-east-1") \
    .config("spark.executorEnv.AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY) \
    .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# Supprimez cette ligne redondante :
# spark.conf.set("spark.executorEnv.AWS_REGION", "us-east-1")
log.info("Gold Processing — Spark session initialized")

# ── DDL Gold Tables ───────────────────────────────────────────────────────────
GOLD_DDL = {

    # ── 1. Sales Facts — grain: one row per order ─────────────────────────────
    # KPIs: order_count, total_orders_by_status, channel_mix, avg_items_per_order
    "gold_sales_facts": """
        CREATE TABLE IF NOT EXISTS iceberg.gold.gold_sales_facts (
            order_date          DATE,
            channel             STRING,
            status              STRING,
            customer_segment    STRING,
            order_count         BIGINT,
            unique_customers    BIGINT,
            total_items         BIGINT,
            avg_items_per_order DOUBLE,
            _computed_at        TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (order_date)
        TBLPROPERTIES (
            'write.format.default'       = 'parquet',
            'write.target-file-size-bytes' = '134217728'
        )
    """,

    # ── 2. Payments Summary — grain: one row per day/method/status ────────────
    # KPIs: total_payments, success_rate, avg_payment_delay_days
    "gold_payments_summary": """
        CREATE TABLE IF NOT EXISTS iceberg.gold.gold_payments_summary (
            payment_date         DATE,
            payment_method       STRING,
            payment_status       STRING,
            payment_count        BIGINT,
            successful_payments  BIGINT,
            failed_payments      BIGINT,
            success_rate_pct     DOUBLE,
            avg_days_to_payment  DOUBLE,
            _computed_at         TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (payment_date)
        TBLPROPERTIES ('write.format.default'='parquet')
    """,

    # ── 3. Returns Summary — grain: one row per day/reason/status ────────────
    # KPIs: return_count, return_rate, top_reasons
    "gold_returns_summary": """
        CREATE TABLE IF NOT EXISTS iceberg.gold.gold_returns_summary (
            return_date         DATE,
            reason              STRING,
            status              STRING,
            return_count        BIGINT,
            unique_orders       BIGINT,
            avg_days_to_return  DOUBLE,
            _computed_at        TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (return_date)
        TBLPROPERTIES ('write.format.default'='parquet')
    """,

    # ── 4. Customer Segments — grain: one row per segment/city ───────────────
    # KPIs: customer_count, order_count, avg_orders_per_customer
    "gold_customer_segments": """
        CREATE TABLE IF NOT EXISTS iceberg.gold.gold_customer_segments (
            segment                  STRING,
            city                     STRING,
            customer_count           BIGINT,
            total_orders             BIGINT,
            avg_orders_per_customer  DOUBLE,
            active_customers         BIGINT,
            _computed_at             TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('write.format.default'='parquet')
    """,

    # ── 5. Daily Revenue — grain: one row per day/channel ────────────────────
    # KPIs: orders_per_day, delivered_count, cancelled_count, completion_rate
    "gold_daily_revenue": """
        CREATE TABLE IF NOT EXISTS iceberg.gold.gold_daily_revenue (
            order_date          DATE,
            channel             STRING,
            total_orders        BIGINT,
            delivered_orders    BIGINT,
            cancelled_orders    BIGINT,
            pending_orders      BIGINT,
            completion_rate_pct DOUBLE,
            cancellation_rate_pct DOUBLE,
            _computed_at        TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (order_date)
        TBLPROPERTIES ('write.format.default'='parquet')
    """,

    # ── 6. Product Performance — grain: one row per category/brand ───────────
    # KPIs: times_ordered, total_items_sold, avg_quantity_per_order
    "gold_product_performance": """
        CREATE TABLE IF NOT EXISTS iceberg.gold.gold_product_performance (
            category              STRING,
            brand                 STRING,
            times_ordered         BIGINT,
            total_items_sold      BIGINT,
            avg_quantity_per_order DOUBLE,
            return_count          BIGINT,
            return_rate_pct       DOUBLE,
            _computed_at          TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('write.format.default'='parquet')
    """,
}


def create_gold_tables():
    log.info("=== Creating namespace iceberg.gold ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    for table_name, ddl in GOLD_DDL.items():
        try:
            spark.sql(ddl)
            log.info(f"  OK iceberg.gold.{table_name}")
        except Exception as e:
            log.error(f"  FAIL iceberg.gold.{table_name}: {e}")
            raise
    log.info("=== All gold tables created ===")
    spark.sql("SHOW TABLES IN iceberg.gold").show()


# ── Lecture des tables Silver ─────────────────────────────────────────────────
def read_silver():
    log.info("Reading Silver tables...")
    customers  = spark.read.format("iceberg").load("iceberg.silver.customers")
    orders     = spark.read.format("iceberg").load("iceberg.silver.orders")
    order_items= spark.read.format("iceberg").load("iceberg.silver.order_items")
    payments   = spark.read.format("iceberg").load("iceberg.silver.payments")
    returns    = spark.read.format("iceberg").load("iceberg.silver.returns")
    products   = spark.read.format("iceberg").load("iceberg.silver.products")
    log.info("Silver tables loaded.")
    return customers, orders, order_items, payments, returns, products


# ── Agrégation 1 : gold_sales_facts ──────────────────────────────────────────
def build_sales_facts(orders, customers, order_items):
    log.info("Building gold_sales_facts...")

    # Join orders → customers pour récupérer le segment
    orders_enriched = orders.alias("o") \
        .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left") \
        .select(
            col("o.order_id"),
            to_date(col("o.order_date")).alias("order_date"),
            col("o.channel"),
            col("o.status"),
            col("c.segment").alias("customer_segment"),
            col("o.customer_id"),
        )

    # Join avec order_items pour compter les articles
    items_per_order = order_items.groupBy("order_id") \
        .agg(count("order_item_id").alias("item_count"))

    orders_with_items = orders_enriched.join(items_per_order, "order_id", "left") \
        .fillna({"item_count": 0})

    # Agrégation finale
    result = orders_with_items.groupBy(
        "order_date", "channel", "status", "customer_segment"
    ).agg(
        count("order_id").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("item_count").alias("total_items"),
        round(avg("item_count"), 2).alias("avg_items_per_order"),
    ).withColumn("_computed_at", current_timestamp())

    log.info(f"gold_sales_facts: {result.count()} rows")
    return result


# ── Agrégation 2 : gold_payments_summary ─────────────────────────────────────
def build_payments_summary(payments, orders):
    log.info("Building gold_payments_summary...")

    # Join payments → orders pour calculer le délai de paiement
    payments_enriched = payments.alias("p") \
        .join(orders.alias("o"), col("p.order_id") == col("o.order_id"), "left") \
        .select(
            to_date(col("p.payment_date")).alias("payment_date"),
            col("p.payment_method"),
            col("p.payment_status"),
            col("p.payment_id"),
            datediff(
                col("p.payment_date").cast("date"),
                col("o.order_date").cast("date")
            ).alias("days_to_payment"),
        )

    result = payments_enriched.groupBy(
        "payment_date", "payment_method", "payment_status"
    ).agg(
        count("payment_id").alias("payment_count"),
        count(when(col("payment_status") == "completed", 1)).alias("successful_payments"),
        count(when(col("payment_status") == "failed", 1)).alias("failed_payments"),
        round(
            count(when(col("payment_status") == "completed", 1)) * 100.0 /
            count("payment_id"), 2
        ).alias("success_rate_pct"),
        round(avg(when(col("days_to_payment") >= 0, col("days_to_payment"))), 2)
            .alias("avg_days_to_payment"),
    ).withColumn("_computed_at", current_timestamp())

    log.info(f"gold_payments_summary: {result.count()} rows")
    return result


# ── Agrégation 3 : gold_returns_summary ──────────────────────────────────────
def build_returns_summary(returns, orders):
    log.info("Building gold_returns_summary...")

    returns_enriched = returns.alias("r") \
        .join(orders.alias("o"), col("r.order_id") == col("o.order_id"), "left") \
        .select(
            to_date(col("r.return_date")).alias("return_date"),
            col("r.reason"),
            col("r.status"),
            col("r.return_id"),
            col("r.order_id"),
            datediff(
                col("r.return_date").cast("date"),
                col("o.order_date").cast("date")
            ).alias("days_to_return"),
        )

    result = returns_enriched.groupBy(
        "return_date", "reason", "status"
    ).agg(
        count("return_id").alias("return_count"),
        countDistinct("order_id").alias("unique_orders"),
        round(avg(when(col("days_to_return") >= 0, col("days_to_return"))), 2)
            .alias("avg_days_to_return"),
    ).withColumn("_computed_at", current_timestamp())

    log.info(f"gold_returns_summary: {result.count()} rows")
    return result


# ── Agrégation 4 : gold_customer_segments ────────────────────────────────────
def build_customer_segments(customers, orders):
    log.info("Building gold_customer_segments...")

    orders_per_customer = orders.groupBy("customer_id") \
        .agg(count("order_id").alias("order_count"))

    customers_enriched = customers.alias("c") \
        .join(orders_per_customer.alias("o"), "customer_id", "left") \
        .fillna({"order_count": 0})

    result = customers_enriched.groupBy("segment", "city").agg(
        count("customer_id").alias("customer_count"),
        sum("order_count").alias("total_orders"),
        round(avg("order_count"), 2).alias("avg_orders_per_customer"),
        count(when(col("order_count") > 0, 1)).alias("active_customers"),
    ).withColumn("_computed_at", current_timestamp())

    log.info(f"gold_customer_segments: {result.count()} rows")
    return result


# ── Agrégation 5 : gold_daily_revenue ────────────────────────────────────────
def build_daily_revenue(orders):
    log.info("Building gold_daily_revenue...")

    result = orders.groupBy(
        to_date(col("order_date")).alias("order_date"),
        "channel"
    ).agg(
        count("order_id").alias("total_orders"),
        count(when(col("status") == "delivered", 1)).alias("delivered_orders"),
        count(when(col("status") == "cancelled", 1)).alias("cancelled_orders"),
        count(when(col("status").isin("pending", "confirmed"), 1)).alias("pending_orders"),
        round(
            count(when(col("status") == "delivered", 1)) * 100.0 /
            count("order_id"), 2
        ).alias("completion_rate_pct"),
        round(
            count(when(col("status") == "cancelled", 1)) * 100.0 /
            count("order_id"), 2
        ).alias("cancellation_rate_pct"),
    ).withColumn("_computed_at", current_timestamp())

    log.info(f"gold_daily_revenue: {result.count()} rows")
    return result


# ── Agrégation 6 : gold_product_performance ───────────────────────────────────
def build_product_performance(products, order_items, returns, orders):
    log.info("Building gold_product_performance...")

    items_sold = order_items.alias("oi") \
        .join(products.alias("p"), col("oi.product_id") == col("p.product_id"), "left") \
        .select(
            col("oi.order_id").alias("oi_order_id"),
            col("oi.order_item_id"),
            col("oi.quantity"),
            coalesce(col("p.category"), lit("Unknown")).alias("category"),
            coalesce(col("p.brand"), lit("Unknown")).alias("brand"),
        )

    returns_per_order = returns.groupBy("order_id") \
        .agg(count("return_id").alias("return_count")) \
        .withColumnRenamed("order_id", "r_order_id")

    items_with_returns = items_sold \
        .join(returns_per_order, col("oi_order_id") == col("r_order_id"), "left") \
        .fillna({"return_count": 0})

    result = items_with_returns.groupBy("category", "brand").agg(
        countDistinct("oi_order_id").alias("times_ordered"),
        sum("quantity").alias("total_items_sold"),
        round(avg("quantity"), 2).alias("avg_quantity_per_order"),
        sum("return_count").alias("return_count"),
        round(
            sum("return_count") * 100.0 / countDistinct("oi_order_id"), 2
        ).alias("return_rate_pct"),
    ).withColumn("_computed_at", current_timestamp())

    log.info(f"gold_product_performance: {result.count()} rows")
    return result




# ── Écriture dans Iceberg (overwrite complet à chaque run) ───────────────────
def write_gold(df, table_name):
    log.info(f"Writing iceberg.gold.{table_name}...")
    df.writeTo(f"iceberg.gold.{table_name}") \
        .option("overwrite-enabled", "true") \
        .overwritePartitions()
    log.info(f"  OK iceberg.gold.{table_name}")


def main():
    log.info("=== Gold Processing starting ===")

    try:
        create_gold_tables()
    except Exception as e:
        log.error(f"Table creation failed: {e}")
        sys.exit(1)

    # Lire Silver
    try:
        customers, orders, order_items, payments, returns, products = read_silver()
    except Exception as e:
        log.error(f"Failed to read Silver tables: {e}")
        sys.exit(1)

    # Construire et écrire chaque table Gold
    jobs = [
        ("gold_sales_facts",        lambda: build_sales_facts(orders, customers, order_items)),
        ("gold_payments_summary",   lambda: build_payments_summary(payments, orders)),
        ("gold_returns_summary",    lambda: build_returns_summary(returns, orders)),
        ("gold_customer_segments",  lambda: build_customer_segments(customers, orders)),
        ("gold_daily_revenue",      lambda: build_daily_revenue(orders)),
        ("gold_product_performance",lambda: build_product_performance(products, order_items, returns, orders)),
    ]

    for table_name, build_fn in jobs:
        try:
            df = build_fn()
            write_gold(df, table_name)
            log.info(f"  DONE {table_name}")
        except Exception as e:
            log.error(f"  FAILED {table_name}: {e}")

    log.info("=== Gold Processing complete ===")
    spark.stop()


if __name__ == "__main__":
    main()