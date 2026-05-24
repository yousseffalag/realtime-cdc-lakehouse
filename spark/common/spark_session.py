import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_spark_session(app_name: str):

    load_dotenv()

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE_PATH", "s3a://lakehouse-bronze/warehouse")
    catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "lakehouse")
    nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")

    spark_master = os.getenv("SPARK_MASTER_URL", "spark://cdc-spark-master:7077")

    packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ])

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(spark_master) \
        .config("spark.jars.packages", packages) \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "1g") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.jars.ivy", "/tmp/.ivy2")

    builder = builder \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.uri", nessie_uri) \
        .config(f"spark.sql.catalog.{catalog_name}.ref", "main") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", iceberg_warehouse) \
        .config("spark.sql.defaultCatalog", catalog_name)

    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return builder.getOrCreate()