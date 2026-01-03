CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.train AS
WITH base AS (
  SELECT
    is_fraud,
    step,
    type,
    amount,
    name_orig,
    oldbalance_orig,
    newbalance_orig,
    name_dest,
    oldbalance_dest,
    newbalance_dest
  FROM silver.base
),

feat AS (
  SELECT
    b.is_fraud,
    b.step,
    b.type,
    b.amount,
    b.name_orig,
    b.name_dest,

    (b.oldbalance_orig - b.newbalance_orig) AS orig_balance_delta,
    ((b.oldbalance_orig - b.newbalance_orig) - b.amount) AS orig_delta_minus_amount,

    (b.newbalance_dest - b.oldbalance_dest) AS dest_balance_delta,
    ((b.newbalance_dest - b.oldbalance_dest) - b.amount) AS dest_delta_minus_amount,

    COALESCE(COUNT(*) OVER w_dest_1h, 0) AS dest_txn_count_1h,
    COALESCE(COUNT(*) OVER w_dest_24h, 0) AS dest_txn_count_24h,

    COALESCE(SUM(b.amount) OVER w_dest_1h, 0.0) AS dest_amount_sum_1h,
    COALESCE(SUM(b.amount) OVER w_dest_24h, 0.0) AS dest_amount_sum_24h,

    MAX(b.step) OVER (
      PARTITION BY b.name_dest
      ORDER BY b.step
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS dest_last_seen_step

  FROM base b

  WINDOW
    w_dest_1h AS (
      PARTITION BY b.name_dest
      ORDER BY b.step
      RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING
    ),
    w_dest_24h AS (
      PARTITION BY b.name_dest
      ORDER BY b.step
      RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
    )
)

SELECT
  is_fraud,
  step,
  type,
  amount,
  name_orig,
  name_dest,

  orig_balance_delta,
  orig_delta_minus_amount,
  dest_balance_delta,
  dest_delta_minus_amount,

  dest_txn_count_1h,
  dest_txn_count_24h,
  dest_amount_sum_1h,
  dest_amount_sum_24h,

  CASE
    WHEN dest_txn_count_24h > 0 THEN dest_amount_sum_24h / dest_txn_count_24h
    ELSE 0.0
  END AS dest_amount_mean_24h,

  CASE
    WHEN dest_last_seen_step IS NULL THEN NULL
    ELSE (step - dest_last_seen_step)
  END AS dest_last_gap_hours
FROM feat;
