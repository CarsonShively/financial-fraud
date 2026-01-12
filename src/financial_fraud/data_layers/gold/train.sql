-- Offline feature creation.

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.train AS
WITH base AS (
  SELECT
    txn_id,
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
    b.txn_id,
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

    COUNT(*) OVER w_dest_1h  AS dest_txn_count_1h,
    COUNT(*) OVER w_dest_24h AS dest_txn_count_24h,

    COALESCE(SUM(b.amount) OVER w_dest_1h, 0.0)   AS dest_amount_sum_1h,
    COALESCE(SUM(b.amount) OVER w_dest_24h, 0.0)  AS dest_amount_sum_24h

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
  txn_id,
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
  dest_amount_sum_24h
FROM feat;
