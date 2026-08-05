-- ============================================
-- FlowServer Demo: Database Creation
-- ============================================
-- Description: Creates the 'streaming_demo' database for the demo
-- Prerequisites: PostgreSQL/WarehousePG running
-- Usage: psql -h localhost -p 5432 -U gpadmin -d postgres -f 01_create_database.sql
-- ============================================

-- Drop database if exists (WARNING: destroys all data)
DROP DATABASE IF EXISTS streaming_demo;

-- Create database
CREATE DATABASE streaming_demo
  WITH OWNER = gpadmin
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       CONNECTION LIMIT = -1;

\echo '✓ Database "streaming_demo" created successfully'

-- Connect to new database
\c streaming_demo

\echo '✓ Connected to database "streaming_demo"'
\echo 'Next step: Run 02_create_tables.sql'
