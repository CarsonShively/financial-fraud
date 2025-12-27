CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.train AS
SELECT
  f.*,
  l.is_fraud
FROM gold.features f
JOIN silver.labels l
  ON l.tx_id = f.tx_id;