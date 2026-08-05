-- =============================================================================
-- PXF External Tables — Stock Prices (ORC + Parquet on MinIO/S3)
--
-- Demonstrates WarehousePG/Greenplum's PXF (Platform Extension Framework)
-- querying data directly from S3-compatible object storage — in TWO columnar
-- formats, from the same underlying dataset — with NO load/copy step. Each
-- external table is a live view over the files.
--
-- PREREQUISITE (infrastructure, not something this script can do): a PXF
-- server profile named 'minio' must already be configured on the cluster
-- (pointing at your MinIO endpoint/credentials) before this will work. That's
-- a one-time platform/DBA setup step — see the PXF S3 connector docs.
--
-- Run with:
--   psql -h localhost -p 5432 -U gpadmin -d streaming_demo -v ON_ERROR_STOP=1 \
--        -f 05_pxf_stock_prices.sql
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pxf;

-- ── ORC variant ───────────────────────────────────────────────────────────────
DROP EXTERNAL TABLE IF EXISTS stock_prices_orc;

CREATE EXTERNAL TABLE stock_prices_orc (
  symbol text,
  date date,  -- because we stored it as a string
  open float8,
  high float8,
  low float8,
  close float8,
  volume int8
)
LOCATION ('pxf://pxf-orc/stock_prices_chunk_0.orc?PROFILE=s3:orc&SERVER=minio')
FORMAT 'custom' (formatter='pxfwritable_import');

-- ── Parquet variant (same data, different columnar format) ────────────────────
DROP EXTERNAL TABLE IF EXISTS stock_prices_parquet;

CREATE EXTERNAL TABLE stock_prices_parquet (
  symbol text,
  date date,
  open float8,
  high float8,
  low float8,
  close float8,
  volume int8
)
LOCATION ('pxf://pxf-parquet/stock_prices_chunk_0.parquet?PROFILE=s3:parquet&SERVER=minio')
FORMAT 'custom' (formatter='pxfwritable_import');

-- Quick sanity check — both should return the same rows
SELECT symbol, date, open, high FROM stock_prices_orc LIMIT 10;
SELECT symbol, date, open, high FROM stock_prices_parquet LIMIT 10;

