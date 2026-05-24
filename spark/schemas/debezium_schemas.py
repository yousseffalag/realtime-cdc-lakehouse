"""
Explicit Debezium envelope schemas for all CDC topics.

Why explicit schemas?
  - Schema inference hits Kafka every batch → adds latency + instability
  - Debezium JSON envelopes are well-known; defining them statically ensures
    the pipeline never crashes on an empty partition or schema evolution.

Debezium envelope structure (value.converter.schemas.enable=false):
{
  "before": { ...row fields... } | null,
  "after":  { ...row fields... } | null,
  "op":     "c" | "u" | "d" | "r",          # create, update, delete, read(snapshot)
  "ts_ms":  1234567890123,
  "source": { "lsn": ..., "txId": ..., ... }
}
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType,
    DoubleType, TimestampType, BooleanType
)

# ── Shared source metadata block ────────────────────────────────────────────
SOURCE_SCHEMA = StructType([
    StructField("version",   StringType(),  True),
    StructField("connector", StringType(),  True),
    StructField("name",      StringType(),  True),
    StructField("ts_ms",     LongType(),    True),
    StructField("snapshot",  StringType(),  True),
    StructField("db",        StringType(),  True),
    StructField("sequence",  StringType(),  True),
    StructField("schema",    StringType(),  True),
    StructField("table",     StringType(),  True),
    StructField("txId",      LongType(),    True),
    StructField("lsn",       LongType(),    True),
    StructField("xmin",      LongType(),    True),
])


# ── Row schemas per table ────────────────────────────────────────────────────

CUSTOMERS_ROW = StructType([
    StructField("customer_id",   IntegerType(), True),
    StructField("name",          StringType(),  True),
    StructField("email",         StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("segment",       StringType(),  True),
    StructField("registered_at", StringType(),  True),   # kept as string; parsed in Silver
])

PRODUCTS_ROW = StructType([
    StructField("product_id",  IntegerType(), True),
    StructField("name",        StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("brand",       StringType(),  True),
    StructField("price",       DoubleType(),  True),
])

ORDERS_ROW = StructType([
    StructField("order_id",    IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date",  StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("channel",     StringType(),  True),
])

ORDER_ITEMS_ROW = StructType([
    StructField("item_id",      IntegerType(), True),
    StructField("order_id",     IntegerType(), True),
    StructField("product_id",   IntegerType(), True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
])

PAYMENTS_ROW = StructType([
    StructField("payment_id",     IntegerType(), True),
    StructField("order_id",       IntegerType(), True),
    StructField("payment_date",   StringType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("status",         StringType(),  True),
])

RETURNS_ROW = StructType([
    StructField("return_id",   IntegerType(), True),
    StructField("order_id",    IntegerType(), True),
    StructField("return_date", StringType(),  True),
    StructField("reason",      StringType(),  True),
    StructField("status",      StringType(),  True),
])


def debezium_envelope(row_schema: StructType) -> StructType:
    """Wraps a row schema in a full Debezium CDC envelope."""
    return StructType([
        StructField("before",  row_schema,   True),
        StructField("after",   row_schema,   True),
        StructField("source",  SOURCE_SCHEMA, True),
        StructField("op",      StringType(), True),
        StructField("ts_ms",   LongType(),   True),
        StructField("transaction", StringType(), True),
    ])


# ── Public registry: topic suffix → envelope schema ─────────────────────────
TOPIC_SCHEMAS = {
    "customers":   debezium_envelope(CUSTOMERS_ROW),
    "products":    debezium_envelope(PRODUCTS_ROW),
    "orders":      debezium_envelope(ORDERS_ROW),
    "order_items": debezium_envelope(ORDER_ITEMS_ROW),
    "payments":    debezium_envelope(PAYMENTS_ROW),
    "returns":     debezium_envelope(RETURNS_ROW),
}