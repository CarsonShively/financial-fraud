CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.orig_snapshot AS
WITH params AS (
  SELECT MAX(step) AS now_step
  FROM silver.base
)
SELECT
  b.name_orig,
  COUNT(*) AS orig_txn_count,
  SUM(b.amount) AS orig_amount_sum,
  SUM(b.newbalance_orig - b.oldbalance_orig) AS orig_balance_delta_sum,
  MAX(b.step) AS orig_last_step,
  CASE WHEN COUNT(*) > 0 THEN SUM(b.amount) / COUNT(*) ELSE 0.0 END AS orig_amount_mean,
  (SELECT now_step FROM params) AS asof_step
FROM silver.base b
WHERE b.step <= (SELECT now_step FROM params)
GROUP BY b.name_orig;

CREATE OR REPLACE TABLE gold.dest_snapshot AS
WITH params AS (
  SELECT MAX(step) AS now_step
  FROM silver.base
)
SELECT
  b.name_dest,
  COUNT(*) AS dest_txn_count,
  SUM(b.amount) AS dest_amount_sum,
  SUM(b.newbalance_dest - b.oldbalance_dest) AS dest_balance_delta_sum,
  MAX(b.step) AS dest_last_step,
  CASE WHEN COUNT(*) > 0 THEN SUM(b.amount) / COUNT(*) ELSE 0.0 END AS dest_amount_mean,
  (SELECT now_step FROM params) AS asof_step
FROM silver.base b
WHERE b.step <= (SELECT now_step FROM params)
GROUP BY b.name_dest;
