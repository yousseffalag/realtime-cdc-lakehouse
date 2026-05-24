-- ============================================================================
-- 📊 TRINO ANALYTICAL SQL PACK — CDC LAKEHOUSE PLATFORM
-- Description: Sample analytical queries to query Bronze, Silver, and Gold layers
--              and calculate the required project metrics and KPIs.
-- ============================================================================

-- ============================================================================
-- 1. INDICATOR: TOTAL CDC EVENTS CAPTURED PER TABLE (Bronze Layer)
-- Description: Checks raw event ingestion counts across Kafka topics.
-- ============================================================================
-- Note: Run these against Trino's 'iceberg' catalog.
SELECT 
    topic, 
    COUNT(*) as total_event_count, 
    MIN(ingestion_timestamp) as first_ingestion_time, 
    MAX(ingestion_timestamp) as last_ingestion_time
FROM (
    SELECT topic, ingestion_timestamp FROM bronze.customers
    UNION ALL
    SELECT topic, ingestion_timestamp FROM bronze.products
    UNION ALL
    SELECT topic, ingestion_timestamp FROM bronze.orders
    UNION ALL
    SELECT topic, ingestion_timestamp FROM bronze.order_items
    UNION ALL
    SELECT topic, ingestion_timestamp FROM bronze.payments
    UNION ALL
    SELECT topic, ingestion_timestamp FROM bronze.returns
)
GROUP BY topic
ORDER BY total_event_count DESC;


-- ============================================================================
-- 2. INDICATOR: MUTATIONS COUNT BREAKDOWN (Insert, Update, Delete)
-- Description: Parses Debezium op code (c=insert, u=update, d=delete, r=snapshot)
--              from raw JSON envelopes in Bronze.
-- ============================================================================
SELECT 
    table_name,
    op_code,
    CASE op_code
        WHEN 'c' THEN 'INSERT'
        WHEN 'u' THEN 'UPDATE'
        WHEN 'd' THEN 'DELETE'
        WHEN 'r' THEN 'SNAPSHOT_READ'
        ELSE 'UNKNOWN'
    END as operation_type,
    COUNT(*) as operation_count
FROM (
    SELECT 'customers' as table_name, coalesce(json_extract_scalar(cast(raw_payload as json), '$.op'), json_extract_scalar(cast(raw_payload as json), '$.payload.op')) as op_code FROM bronze.customers
    UNION ALL
    SELECT 'products' as table_name, coalesce(json_extract_scalar(cast(raw_payload as json), '$.op'), json_extract_scalar(cast(raw_payload as json), '$.payload.op')) as op_code FROM bronze.products
    UNION ALL
    SELECT 'orders' as table_name, coalesce(json_extract_scalar(cast(raw_payload as json), '$.op'), json_extract_scalar(cast(raw_payload as json), '$.payload.op')) as op_code FROM bronze.orders
    UNION ALL
    SELECT 'order_items' as table_name, coalesce(json_extract_scalar(cast(raw_payload as json), '$.op'), json_extract_scalar(cast(raw_payload as json), '$.payload.op')) as op_code FROM bronze.order_items
    UNION ALL
    SELECT 'payments' as table_name, coalesce(json_extract_scalar(cast(raw_payload as json), '$.op'), json_extract_scalar(cast(raw_payload as json), '$.payload.op')) as op_code FROM bronze.payments
    UNION ALL
    SELECT 'returns' as table_name, coalesce(json_extract_scalar(cast(raw_payload as json), '$.op'), json_extract_scalar(cast(raw_payload as json), '$.payload.op')) as op_code FROM bronze.returns
)
GROUP BY table_name, op_code
ORDER BY table_name, operation_count DESC;


-- ============================================================================
-- 3. INDICATOR: LIVE END-TO-END INGESTION LATENCY (Bronze Layer)
-- Description: Measures millisecond delay between database write (source.ts_ms)
--              and landing on object storage (ingestion_timestamp).
-- ============================================================================
SELECT 
    table_name,
    AVG(latency_seconds) as avg_latency_sec,
    MIN(latency_seconds) as min_latency_sec,
    MAX(latency_seconds) as max_latency_sec,
    COUNT(*) as total_events_checked
FROM (
    SELECT 
        'customers' as table_name,
        to_milliseconds(ingestion_timestamp - from_unixtime(cast(coalesce(
            json_extract_scalar(cast(raw_payload as json), '$.source.ts_ms'),
            json_extract_scalar(cast(raw_payload as json), '$.payload.source.ts_ms')
        ) as bigint) / 1000.0)) / 1000.0 as latency_seconds
    FROM bronze.customers
    WHERE raw_payload IS NOT NULL
)
GROUP BY table_name;


-- ============================================================================
-- 4. INDICATOR: ORDER PLACEMENT COUNT EVOLUTION (Silver Layer)
-- Description: Monitors growth in conformed orders grouped by day and status.
-- ============================================================================
SELECT 
    date_trunc('day', order_date) as order_day,
    status,
    COUNT(*) as total_orders
FROM silver.orders
GROUP BY date_trunc('day', order_date), status
ORDER BY order_day DESC, total_orders DESC;


-- ============================================================================
-- 5. INDICATOR: TOTAL SALES REVENUE & AVERAGE ORDER VALUE (Gold Layer)
-- Description: Measures overall financial stats from Gold facts, excluding cancellations.
-- ============================================================================
SELECT 
    SUM(total_amount) as total_gross_revenue,
    COUNT(DISTINCT order_id) as unique_orders_billed,
    SUM(total_amount) / COUNT(DISTINCT order_id) as average_order_value,
    SUM(quantity) as total_items_sold
FROM gold.gold_sales_facts
WHERE order_status != 'cancelled';


-- ============================================================================
-- 6. INDICATOR: PAYMENT METHOD BREAKDOWN (Gold Layer)
-- Description: Evaluates popular billing options and transaction successes.
-- ============================================================================
SELECT 
    payment_method,
    payment_status,
    total_payment_amount,
    payment_count
FROM gold.gold_payments_summary
ORDER BY total_payment_amount DESC;


-- ============================================================================
-- 7. INDICATOR: RETURNS RATE AND REASONS ANALYSIS (Gold Layer)
-- Description: Identifies high return categories and rate percentages.
-- ============================================================================
SELECT 
    reason as return_reason,
    return_status,
    total_returns,
    (CAST(total_returns as double) / (SELECT SUM(total_returns) FROM gold.gold_returns_summary)) * 100.0 as return_reason_percentage
FROM gold.gold_returns_summary
ORDER BY total_returns DESC;


-- ============================================================================
-- 8. INDICATOR: MULTI-DIMENSIONAL REVENUE SEGMENTATION (Gold Layer)
-- Description: Analyzes revenue streams across segment, category, and channels.
-- ============================================================================
SELECT 
    customer_segment,
    order_channel,
    product_category,
    SUM(quantity) as volume_sold,
    SUM(total_amount) as absolute_revenue
FROM gold.gold_sales_facts
GROUP BY customer_segment, order_channel, product_category
ORDER BY absolute_revenue DESC;
