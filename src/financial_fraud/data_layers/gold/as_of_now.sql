CREATE OR REPLACE TABLE gold.dest_buckets_24 AS
WITH per_step AS (
  SELECT
    name_dest,
    step,
    COUNT(*)::INT AS cnt,
    SUM(amount)::DOUBLE AS amt_sum
  FROM silver.base
  GROUP BY 1, 2
),
latest AS (
  SELECT
    name_dest,
    MAX(step) AS cur_step
  FROM per_step
  GROUP BY 1
)
SELECT
  l.name_dest,
  l.cur_step AS dest_last_seen_step,

  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 1  THEN p.cnt END), 0)  AS dest_cnt_b1,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 2  THEN p.cnt END), 0)  AS dest_cnt_b2,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 3  THEN p.cnt END), 0)  AS dest_cnt_b3,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 4  THEN p.cnt END), 0)  AS dest_cnt_b4,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 5  THEN p.cnt END), 0)  AS dest_cnt_b5,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 6  THEN p.cnt END), 0)  AS dest_cnt_b6,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 7  THEN p.cnt END), 0)  AS dest_cnt_b7,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 8  THEN p.cnt END), 0)  AS dest_cnt_b8,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 9  THEN p.cnt END), 0)  AS dest_cnt_b9,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 10 THEN p.cnt END), 0)  AS dest_cnt_b10,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 11 THEN p.cnt END), 0)  AS dest_cnt_b11,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 12 THEN p.cnt END), 0)  AS dest_cnt_b12,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 13 THEN p.cnt END), 0)  AS dest_cnt_b13,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 14 THEN p.cnt END), 0)  AS dest_cnt_b14,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 15 THEN p.cnt END), 0)  AS dest_cnt_b15,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 16 THEN p.cnt END), 0)  AS dest_cnt_b16,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 17 THEN p.cnt END), 0)  AS dest_cnt_b17,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 18 THEN p.cnt END), 0)  AS dest_cnt_b18,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 19 THEN p.cnt END), 0)  AS dest_cnt_b19,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 20 THEN p.cnt END), 0)  AS dest_cnt_b20,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 21 THEN p.cnt END), 0)  AS dest_cnt_b21,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 22 THEN p.cnt END), 0)  AS dest_cnt_b22,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 23 THEN p.cnt END), 0)  AS dest_cnt_b23,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 24 THEN p.cnt END), 0)  AS dest_cnt_b24,

  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 1  THEN p.amt_sum END), 0.0) AS dest_sum_b1,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 2  THEN p.amt_sum END), 0.0) AS dest_sum_b2,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 3  THEN p.amt_sum END), 0.0) AS dest_sum_b3,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 4  THEN p.amt_sum END), 0.0) AS dest_sum_b4,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 5  THEN p.amt_sum END), 0.0) AS dest_sum_b5,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 6  THEN p.amt_sum END), 0.0) AS dest_sum_b6,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 7  THEN p.amt_sum END), 0.0) AS dest_sum_b7,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 8  THEN p.amt_sum END), 0.0) AS dest_sum_b8,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 9  THEN p.amt_sum END), 0.0) AS dest_sum_b9,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 10 THEN p.amt_sum END), 0.0) AS dest_sum_b10,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 11 THEN p.amt_sum END), 0.0) AS dest_sum_b11,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 12 THEN p.amt_sum END), 0.0) AS dest_sum_b12,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 13 THEN p.amt_sum END), 0.0) AS dest_sum_b13,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 14 THEN p.amt_sum END), 0.0) AS dest_sum_b14,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 15 THEN p.amt_sum END), 0.0) AS dest_sum_b15,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 16 THEN p.amt_sum END), 0.0) AS dest_sum_b16,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 17 THEN p.amt_sum END), 0.0) AS dest_sum_b17,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 18 THEN p.amt_sum END), 0.0) AS dest_sum_b18,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 19 THEN p.amt_sum END), 0.0) AS dest_sum_b19,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 20 THEN p.amt_sum END), 0.0) AS dest_sum_b20,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 21 THEN p.amt_sum END), 0.0) AS dest_sum_b21,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 22 THEN p.amt_sum END), 0.0) AS dest_sum_b22,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 23 THEN p.amt_sum END), 0.0) AS dest_sum_b23,
  COALESCE(MAX(CASE WHEN p.step = l.cur_step - 24 THEN p.amt_sum END), 0.0) AS dest_sum_b24

FROM latest l
LEFT JOIN per_step p
  ON p.name_dest = l.name_dest
 AND p.step BETWEEN l.cur_step - 24 AND l.cur_step - 1
GROUP BY 1, 2;
