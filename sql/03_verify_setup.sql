-- ============================================
-- FlowServer Demo: Verify Setup
-- ============================================
-- Description: Verifies database setup and shows current data
-- Usage: psql -h localhost -p 5432 -U gpadmin -d streaming_demo -f 03_verify_setup.sql
-- ============================================

\echo '========================================'
\echo 'FlowServer Demo - Setup Verification'
\echo '========================================'
\echo ''

-- Check database connection
\echo 'Current database:'
SELECT current_database();
\echo ''

-- Check tables exist
\echo 'Tables in public schema:'
\dt public.*
\echo ''

-- Check E-Commerce table structure
\echo 'E-Commerce Orders table structure:'
\d public.ecommerce_orders
\echo ''

-- Check IoT table structure
\echo 'IoT Sensor Readings table structure:'
\d public.iot_sensor_readings
\echo ''

-- Count records
\echo 'Current record counts:'
SELECT 
    'ecommerce_orders' AS table_name,
    COUNT(*) AS record_count,
    MIN(timestamp) AS oldest_record,
    MAX(timestamp) AS newest_record
FROM public.ecommerce_orders
UNION ALL
SELECT 
    'iot_sensor_readings' AS table_name,
    COUNT(*) AS record_count,
    MIN(timestamp) AS oldest_record,
    MAX(timestamp) AS newest_record
FROM public.iot_sensor_readings;
\echo ''

-- Sample E-Commerce data
\echo 'Sample E-Commerce orders (latest 5):'
SELECT 
    order_id,
    timestamp,
    customer_name,
    product_name,
    total_price,
    revenue_bucket
FROM public.ecommerce_orders
ORDER BY timestamp DESC
LIMIT 5;
\echo ''

-- Sample IoT data
\echo 'Sample IoT sensor readings (latest 5):'
SELECT 
    timestamp,
    sensor_id,
    building,
    floor,
    temperature,
    temperature_f,
    comfort_index,
    alert_level
FROM public.iot_sensor_readings
ORDER BY timestamp DESC
LIMIT 5;
\echo ''

-- E-Commerce statistics
\echo 'E-Commerce revenue bucket distribution:'
SELECT 
    revenue_bucket,
    COUNT(*) AS order_count,
    ROUND(AVG(total_price), 2) AS avg_order_value,
    ROUND(SUM(total_price), 2) AS total_revenue
FROM public.ecommerce_orders
GROUP BY revenue_bucket
ORDER BY 
    CASE revenue_bucket
        WHEN 'low' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'high' THEN 3
        WHEN 'premium' THEN 4
    END;
\echo ''

-- IoT statistics
\echo 'IoT sensor alert distribution:'
SELECT 
    alert_level,
    CASE alert_level
        WHEN 0 THEN 'Normal'
        WHEN 1 THEN 'Warning'
        WHEN 2 THEN 'Low Battery'
        ELSE 'Critical'
    END AS alert_description,
    COUNT(*) AS reading_count
FROM public.iot_sensor_readings
GROUP BY alert_level
ORDER BY alert_level;
\echo ''

\echo '========================================'
\echo 'Verification complete!'
\echo '========================================'
