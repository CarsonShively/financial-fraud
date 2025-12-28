CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.base AS
WITH raw AS (
  SELECT
    isFraud,
    step,
    type,
    amount,
    nameOrig,
    oldbalanceOrg,
    newbalanceOrig,
    nameDest,
    oldbalanceDest,
    newbalanceDest
  FROM bronze.raw
),
typed AS (
  SELECT
    TRY_CAST(isFraud AS BIGINT) AS is_fraud,

    TRY_CAST(step AS BIGINT) AS step,
    NULLIF(lower(trim(CAST(type AS VARCHAR))), '') AS type,
    TRY_CAST(amount AS DOUBLE) AS amount,

    NULLIF(trim(CAST(nameOrig AS VARCHAR)), '') AS name_orig,
    TRY_CAST(oldbalanceOrg AS DOUBLE)   AS oldbalance_orig,
    TRY_CAST(newbalanceOrig AS DOUBLE)  AS newbalance_orig,

    NULLIF(trim(CAST(nameDest AS VARCHAR)), '') AS name_dest,
    TRY_CAST(oldbalanceDest AS DOUBLE)  AS oldbalance_dest,
    TRY_CAST(newbalanceDest AS DOUBLE)  AS newbalance_dest
  FROM raw
),
validated AS (
  SELECT
    * REPLACE (
      CASE
        WHEN is_fraud IN (0, 1) THEN is_fraud
        ELSE NULL
      END AS is_fraud,

      CASE
        WHEN type IN ('payment','transfer','cash_out','debit','cash_in') THEN type
        ELSE NULL
      END AS type,

      CASE WHEN step   IS NOT NULL AND step   >= 0 THEN step   ELSE NULL END AS step,
      CASE WHEN amount IS NOT NULL AND amount >= 0 THEN amount ELSE NULL END AS amount,

      CASE WHEN oldbalance_orig IS NOT NULL AND oldbalance_orig >= 0 THEN oldbalance_orig ELSE NULL END AS oldbalance_orig,
      CASE WHEN newbalance_orig IS NOT NULL AND newbalance_orig >= 0 THEN newbalance_orig ELSE NULL END AS newbalance_orig,
      CASE WHEN oldbalance_dest IS NOT NULL AND oldbalance_dest >= 0 THEN oldbalance_dest ELSE NULL END AS oldbalance_dest,
      CASE WHEN newbalance_dest IS NOT NULL AND newbalance_dest >= 0 THEN newbalance_dest ELSE NULL END AS newbalance_dest
    )
  FROM typed
)
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
FROM validated;