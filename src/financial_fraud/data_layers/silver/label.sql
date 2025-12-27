CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.labels AS
WITH raw AS (
  SELECT
    tx_id,
    isFraud
  FROM silver.keyed
),
typed AS (
  SELECT
    tx_id,
    TRY_CAST(isFraud AS BIGINT) AS is_fraud_raw
  FROM raw
),
validated AS (
  SELECT
    tx_id,
    CASE
      WHEN is_fraud_raw IN (0, 1) THEN is_fraud_raw
      ELSE NULL
    END AS is_fraud
  FROM typed
)
SELECT
  tx_id,
  is_fraud
FROM validated;
