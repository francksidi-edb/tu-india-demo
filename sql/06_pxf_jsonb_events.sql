-- =============================================================================
-- PXF External Table — News & Sentiment Events (JSON on MinIO/S3)
--
-- A separate dataset from the stock price tables (05_pxf_stock_prices.sql):
-- per-symbol news headlines with a sentiment label, read live from a JSON
-- file via PXF's s3:json profile — same "query files in place" pattern,
-- different bucket, different format.
--
-- PREREQUISITE: the pxf extension and a PXF server profile named 'minio'
-- must already be set up (see 05_pxf_stock_prices.sql for the one-time
-- CREATE EXTENSION pxf step, only needed once per database).
--
-- Run with:
--   psql -h localhost -p 5432 -U gpadmin -d streaming_demo -v ON_ERROR_STOP=1 \
--        -f 06_pxf_jsonb_events.sql
-- =============================================================================

DROP EXTERNAL TABLE IF EXISTS jsonb_events;

CREATE EXTERNAL TABLE jsonb_events (
  symbol TEXT,
  date TEXT,
  headline TEXT,
  sentiment TEXT,
  close FLOAT
)
LOCATION('pxf://pxf-bucket/clean_jsonb_data_chunk_1.json?PROFILE=s3:json&SERVER=minio')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- Quick sanity check
SELECT symbol, date, headline, sentiment, close FROM jsonb_events LIMIT 10;
