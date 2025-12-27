CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.features AS
WITH base AS (
  SELECT
    tx_id,
    step,
    type,
    amount,
    name_orig,
    name_dest
  FROM silver.base
),

feat AS (
  SELECT
    b.tx_id,
    b.step,
    b.type,
    b.amount,
    b.name_orig,
    b.name_dest,

    -- ----------------------------
    -- ORIG (sender) aggregates
    -- ----------------------------

    COALESCE(
      COUNT(*) OVER (
        PARTITION BY b.name_orig
        ORDER BY b.step
        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS orig_tx_count_24h,

    COALESCE(
      SUM(b.amount) OVER (
        PARTITION BY b.name_orig
        ORDER BY b.step
        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS orig_tx_amount_sum_24h,

    COALESCE(
      b.step
      - LAG(b.step) OVER (
          PARTITION BY b.name_orig
          ORDER BY b.step
        ),
      999999
    ) AS orig_time_since_last_tx,

    COALESCE(
      AVG(CASE WHEN b.type = 'TRANSFER' THEN 1.0 ELSE 0.0 END) OVER (
        PARTITION BY b.name_orig
        ORDER BY b.step
        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS orig_frac_transfer_24h,

    -- ----------------------------
    -- DEST (receiver) aggregates
    -- ----------------------------

    COALESCE(
      COUNT(*) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS dest_tx_count_1h,

    COALESCE(
      COUNT(*) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS dest_tx_count_24h,

    COALESCE(
      SUM(b.amount) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS dest_tx_amount_sum_1h,

    COALESCE(
      AVG(b.amount) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS dest_tx_amount_mean_24h,

    COALESCE(
      AVG(CASE WHEN b.type = 'TRANSFER' THEN 1.0 ELSE 0.0 END) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
      ),
      0.0
    ) AS dest_frac_transfer_24h,

    COALESCE(
      approx_count_distinct(b.name_orig) OVER (
        PARTITION BY b.name_dest
        ORDER BY b.step
        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS dest_unique_senders_24h

  FROM base b
)

SELECT *
FROM feat;
