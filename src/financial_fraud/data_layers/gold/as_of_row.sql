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

    -- ----------------------------
    -- ORIG (sender) cumulative state (as-of-row)
    -- ----------------------------
    COALESCE(
      COUNT(*) OVER (
        PARTITION BY b.name_orig
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS orig_txn_count,

    COALESCE(
      SUM(b.amount) OVER (
        PARTITION BY b.name_orig
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS orig_amount_sum,

    COALESCE(
      SUM(b.newbalance_orig - b.oldbalance_orig) OVER (
        PARTITION BY b.name_orig
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS orig_balance_delta_sum,

    COALESCE(
      MAX(b.step) OVER (
        PARTITION BY b.name_orig
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      NULL
    ) AS orig_last_step,

    -- ----------------------------
    -- DEST (receiver) cumulative state (as-of-row)
    -- ----------------------------
    COALESCE(
      COUNT(*) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS dest_txn_count,

    COALESCE(
      SUM(b.amount) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS dest_amount_sum,

    COALESCE(
      SUM(b.newbalance_dest - b.oldbalance_dest) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS dest_balance_delta_sum,

    COALESCE(
      MAX(b.step) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      NULL
    ) AS dest_last_step

  FROM base b
)

SELECT
  *,
  -- ----------------------------
  -- Derived means (avoid divide-by-zero)
  -- ----------------------------
  CASE
    WHEN orig_txn_count > 0 THEN orig_amount_sum / orig_txn_count
    ELSE 0.0
  END AS orig_amount_mean,

  CASE
    WHEN dest_txn_count > 0 THEN dest_amount_sum / dest_txn_count
    ELSE 0.0
  END AS dest_amount_mean
FROM feat;
