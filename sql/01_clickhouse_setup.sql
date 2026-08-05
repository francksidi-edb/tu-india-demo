-- =============================================================================
-- ClickHouse: real-time ingestion of the `ecommerce-orders` Kafka topic
-- Run with: clickhouse-client --port 9094 --multiquery < clickhouse_ecommerce_setup.sql
--
-- This consumes the SAME Kafka topic FlowServer already reads into WarehousePG,
-- but as its OWN independent consumer group — so it doesn't compete with or
-- affect FlowServer's offsets/partitions at all. Two consumers, one topic.
-- =============================================================================

-- 1) Kafka "queue" table — ClickHouse polls this topic in the background.
--    This table holds NO data itself; querying it directly consumes messages,
--    so never SELECT from this one — only the materialized view below reads it.
CREATE TABLE IF NOT EXISTS default.kafka_ecommerce_orders
(
    order_id        String,
    timestamp       String,   -- parsed into a real DateTime in the MV below
    customer_id     String,
    customer_name   String,
    customer_email  String,
    product_id      String,
    product_name    String,
    category        String,
    quantity        UInt32,
    unit_price      Float64,
    total_price     Float64,
    payment_method  String,
    country         String,
    city            String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'ecommerce-orders',
    kafka_group_name = 'clickhouse_ecommerce_orders_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

-- 2) Target table — the actual queryable data, with the same computed columns
--    FlowServer's YAML job applies (revenue_bucket, is_bulk_order), so this
--    looks consistent with what's in WarehousePG.
CREATE TABLE IF NOT EXISTS default.ecommerce_orders
(
    order_id        String,
    timestamp       DateTime64(3),
    customer_id     String,
    customer_name   String,
    customer_email  String,
    product_id      String,
    product_name    String,
    category        String,
    quantity        UInt32,
    unit_price      Float64,
    total_price     Float64,
    payment_method  String,
    country         String,
    city            String,
    revenue_bucket  String,
    is_bulk_order   UInt8,
    processing_time DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (order_id, timestamp);

-- 3) Materialized view — the glue. Fires on every batch ClickHouse reads from
--    Kafka, transforms it, and writes into the target table above.
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_ecommerce_orders
TO default.ecommerce_orders
AS SELECT
    order_id,
    parseDateTimeBestEffort(timestamp) AS timestamp,
    customer_id,
    customer_name,
    customer_email,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    total_price,
    payment_method,
    country,
    city,
    multiIf(
        total_price >= 1000, 'premium',
        total_price >= 500,  'high',
        total_price >= 100,  'medium',
        'low'
    ) AS revenue_bucket,
    if(quantity > 2, 1, 0) AS is_bulk_order,
    now64(3) AS processing_time
FROM default.kafka_ecommerce_orders;

