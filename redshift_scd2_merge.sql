BEGIN;

WITH staged AS (
  SELECT *,
         md5(concat_ws('|', customer_id, region)) AS row_hash,
         GETDATE() AS load_ts
  FROM staging_mydataset
),
changed AS (
  SELECT s.*
  FROM staged s
  JOIN customer_dim t
    ON s.customer_id = t.customer_id
   AND t.is_current = TRUE
  WHERE s.row_hash <> t.row_hash
)

UPDATE customer_dim
SET effective_to = (SELECT load_ts FROM staged LIMIT 1),
    is_current = FALSE
WHERE customer_id IN (SELECT customer_id FROM changed)
  AND is_current = TRUE;

INSERT INTO customer_dim (
  customer_id, region, effective_from, effective_to,
  is_current, row_hash
)
SELECT customer_id, region, load_ts, NULL, TRUE, row_hash
FROM changed;

INSERT INTO customer_dim (
  customer_id, region, effective_from, effective_to,
  is_current, row_hash
)
SELECT s.customer_id, s.region, s.load_ts, NULL, TRUE, s.row_hash
FROM staged s
LEFT JOIN customer_dim t
       ON s.customer_id = t.customer_id
WHERE t.customer_id IS NULL;

END;
