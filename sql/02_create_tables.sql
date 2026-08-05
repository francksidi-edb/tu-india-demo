-- ============================================
-- FlowServer Demo: Table Creation
-- ============================================
-- Description: Creates tables for E-Commerce and IoT streams
-- Prerequisites: Database 'streaming_demo' must exist
-- Usage: psql -h localhost -p 5432 -U gpadmin -d streaming_demo -f 02_create_tables.sql
-- ============================================

\echo 'Creating tables for FlowServer demo...'

-- ============================================
-- E-Commerce Orders Table
-- ============================================
DROP TABLE IF EXISTS public.ecommerce_orders CASCADE;

CREATE TABLE public.ecommerce_orders (
    order_id VARCHAR(50) ,
    timestamp TIMESTAMP NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    product_id VARCHAR(20),
    product_name VARCHAR(200),
    category VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(100),
    revenue_bucket VARCHAR(20),    -- Computed: low/medium/high/premium
    is_bulk_order BOOLEAN,          -- Computed: quantity > 2
    processing_time TIMESTAMP       -- Computed: NOW()
) 
with (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (order_id);

\echo '✓ Table "ecommerce_orders" created with indexes'

-- ============================================
-- IoT Sensor Readings Table
-- ============================================
DROP TABLE IF EXISTS public.iot_sensor_readings CASCADE;

CREATE TABLE public.iot_sensor_readings (
    id SERIAL,
    timestamp TIMESTAMP NOT NULL,
    sensor_id VARCHAR(20) NOT NULL,
    location VARCHAR(100),
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    pressure DECIMAL(6,2),
    battery_level DECIMAL(5,2),
    status VARCHAR(20),
    alert_level INTEGER,            -- Computed: 0-3 based on status
    building VARCHAR(50),           -- Computed: extracted from location
    floor INTEGER,                  -- Computed: extracted from location
    temperature_f DECIMAL(5,2),     -- Computed: Celsius to Fahrenheit
    comfort_index VARCHAR(20),      -- Computed: comfortable/acceptable/uncomfortable
    battery_status VARCHAR(20),     -- Computed: good/medium/low/critical
    processed_at TIMESTAMP,         -- Computed: NOW()
    data_quality VARCHAR(20)        -- Computed: valid/suspicious
) 
with (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (sensor_id);


\echo '✓ Table "iot_sensor_readings" created with indexes'

-- ============================================
-- Grant Permissions
-- ============================================
GRANT ALL ON TABLE public.ecommerce_orders TO gpadmin;
GRANT ALL ON TABLE public.iot_sensor_readings TO gpadmin;
GRANT ALL ON SEQUENCE public.iot_sensor_readings_id_seq TO gpadmin;

\echo '✓ Permissions granted'

-- ============================================
-- Verify Tables
-- ============================================
\echo ''
\echo 'Tables created successfully:'
\dt public.*

\echo ''
\echo 'Next step: Create Kafka topics and submit FlowServer jobs'
