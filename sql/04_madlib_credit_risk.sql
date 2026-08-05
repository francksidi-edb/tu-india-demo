-- =============================================================================
-- MADlib Credit Risk Scoring — WarehousePG (Greenplum-based, tu database)
--
-- Trains an in-database logistic regression (Apache MADlib) predicting the
-- probability that a credit account is delinquent, using the CDC'd bureau
-- data already landing in tu_bureau_demo via Debezium -> Kafka -> FlowServer.
-- No data ever leaves the database for this — it's scored where it lands.
--
-- Idempotent / re-runnable: safe to run again any time (e.g. after Simulate
-- Update on the CDC tab, or a batch of new test rows) to retrain on the
-- latest data. Run with:
--   psql -h localhost -p 5432 -U gpadmin -d tu -v ON_ERROR_STOP=1 -f 04_madlib_credit_risk.sql
--
-- NOTE ON MADLIB VERSION: this uses madlib.logregr_train() for training —
-- stable across MADlib versions — but scores manually via the sigmoid
-- formula (1 / (1 + exp(-coef . features))) rather than calling
-- madlib.logregr_predict(), since that function returns a BOOLEAN
-- (predicted class) in some MADlib builds rather than a probability,
-- which would otherwise fail with "operator does not exist: boolean >= numeric".
-- NOTE: MADlib here is installed via madpack, not as a formal Postgres
-- extension — no CREATE EXTENSION needed (and it would error if attempted).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1) Latest snapshot per account / per consumer.
--    credit_accounts and bureau_score_events are both insert-only (append-only
--    change history from CDC) — DISTINCT ON collapses each to its current state.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW tu_bureau_demo.credit_accounts_latest AS
SELECT DISTINCT ON (account_id) *
FROM tu_bureau_demo.credit_accounts
ORDER BY account_id, event_ts DESC NULLS LAST, ingested_ts DESC;

CREATE OR REPLACE VIEW tu_bureau_demo.bureau_score_latest AS
SELECT DISTINCT ON (consumer_id) *
FROM tu_bureau_demo.bureau_score_events
ORDER BY consumer_id, score_ts DESC NULLS LAST, ingested_ts DESC;

-- -----------------------------------------------------------------------------
-- 2) Feature table + label.
--    Label: 1 if the account is currently delinquent (60+ days past due, or
--    status already flagged delinquent), else 0.
--    Features: an intercept term, outstanding/sanctioned ratio, log sanctioned
--    amount, latest bureau score (or a neutral 650 if none yet), account age
--    in days, and one-hot flags for product (line_of_credit is the omitted
--    reference category, standard practice to avoid collinearity).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS tu_bureau_demo.credit_risk_features;
CREATE TABLE tu_bureau_demo.credit_risk_features AS
SELECT
    ca.account_id,
    ca.consumer_id,
    CASE WHEN ca.dpd_bucket IN ('60', '90+') OR ca.status = 'delinquent'
         THEN 1 ELSE 0 END AS label,
    ARRAY[
        1.0,                                                                       -- intercept
        COALESCE(ca.outstanding_amount / NULLIF(ca.sanctioned_amount, 0), 0)::float8, -- utilization
        LN(GREATEST(ca.sanctioned_amount, 1))::float8,                             -- log(sanctioned_amount)
        COALESCE(bs.score_value, 650)::float8,                                     -- latest bureau score
        GREATEST(EXTRACT(DAY FROM now() - ca.opened_ts), 0)::float8,               -- account age (days)
        (CASE WHEN ca.product = 'credit_card'    THEN 1 ELSE 0 END)::float8,
        (CASE WHEN ca.product = 'personal_loan'  THEN 1 ELSE 0 END)::float8,
        (CASE WHEN ca.product = 'auto_loan'      THEN 1 ELSE 0 END)::float8,
        (CASE WHEN ca.product = 'mortgage'       THEN 1 ELSE 0 END)::float8
        -- 'line_of_credit' omitted — reference category
    ]::float8[] AS features
FROM tu_bureau_demo.credit_accounts_latest ca
LEFT JOIN tu_bureau_demo.bureau_score_latest bs ON bs.consumer_id = ca.consumer_id;

-- -----------------------------------------------------------------------------
-- 3) Train — MADlib logistic regression.
--    Skips training with a clear NOTICE if there isn't enough label variety
--    yet (e.g. right after a fresh reset with only a handful of rows) rather
--    than letting MADlib fail on a degenerate single-class dataset.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    n_rows    bigint;
    n_classes int;
BEGIN
    SELECT count(*) INTO n_rows FROM tu_bureau_demo.credit_risk_features;
    SELECT count(DISTINCT label) INTO n_classes FROM tu_bureau_demo.credit_risk_features;

    IF n_rows < 20 THEN
        RAISE NOTICE 'Only % row(s) in credit_risk_features — insert more test rows '
                      'on EPAS (CDC tab) before training for a meaningful model.', n_rows;
    ELSIF n_classes < 2 THEN
        RAISE NOTICE 'All % row(s) share the same label — need both delinquent and '
                      'non-delinquent accounts to train logistic regression. Use '
                      'Simulate Update on the CDC tab to create some delinquent rows.', n_rows;
    ELSE
        DROP TABLE IF EXISTS tu_bureau_demo.credit_risk_model;
        DROP TABLE IF EXISTS tu_bureau_demo.credit_risk_model_summary;

        PERFORM madlib.logregr_train(
            'tu_bureau_demo.credit_risk_features',  -- source table
            'tu_bureau_demo.credit_risk_model',     -- output table
            'label',                                -- dependent variable
            'features',                             -- independent variable (array)
            NULL,                                   -- no grouping
            20,                                      -- max iterations
            'irls'                                  -- optimizer
        );
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- 4) Score — apply the trained model to every account.
--    NOTE: madlib.logregr_predict() returns a BOOLEAN (predicted class at a
--    0.5 threshold) in some MADlib builds, not a probability — that's the
--    "operator does not exist: boolean >= numeric" error if you hit it. To
--    stay portable across MADlib versions, this computes the sigmoid
--    manually from the raw coefficients instead of relying on any particular
--    predict-function signature: probability = 1 / (1 + exp(-coef . features)).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS tu_bureau_demo.credit_risk_scores;
DO $$
BEGIN
    IF to_regclass('tu_bureau_demo.credit_risk_model') IS NOT NULL THEN
        CREATE TABLE tu_bureau_demo.credit_risk_scores AS
        SELECT
            f.account_id,
            f.consumer_id,
            f.label AS actual_delinquent,
            (1.0 / (1.0 + exp(-dp.dot)))::float8 AS probability_delinquent,
            CASE
                WHEN 1.0 / (1.0 + exp(-dp.dot)) >= 0.7 THEN 'high'
                WHEN 1.0 / (1.0 + exp(-dp.dot)) >= 0.4 THEN 'medium'
                ELSE 'low'
            END AS risk_tier
        FROM tu_bureau_demo.credit_risk_features f
        CROSS JOIN tu_bureau_demo.credit_risk_model m
        CROSS JOIN LATERAL (
            SELECT sum(a * b) AS dot FROM unnest(m.coef, f.features) AS t(a, b)
        ) dp;
    ELSE
        CREATE TABLE tu_bureau_demo.credit_risk_scores (
            account_id bigint, consumer_id bigint, actual_delinquent int,
            probability_delinquent float8, risk_tier text
        );
        RAISE NOTICE 'No model was trained this run — credit_risk_scores created empty. '
                      'See the NOTICE above for why, then re-run this script.';
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- 5) Quick sanity check output
-- -----------------------------------------------------------------------------
SELECT risk_tier, count(*) AS accounts, round(avg(probability_delinquent)::numeric, 3) AS avg_probability
FROM tu_bureau_demo.credit_risk_scores
GROUP BY risk_tier
ORDER BY risk_tier;

