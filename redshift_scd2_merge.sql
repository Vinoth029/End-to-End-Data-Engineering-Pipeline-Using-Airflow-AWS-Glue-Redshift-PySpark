BEGIN;

WITH staged AS (
  SELECT *,
         md5(concat_ws('|', business_key, attr1, attr2)) AS row_hash,
         GETDATE() AS load_ts
  FROM staging_mydataset
),
changed AS (
  SELECT s.*
  FROM staged s
  JOIN target_mydataset t
    ON s.business_key = t.business_key
   AND t.is_current = TRUE
  WHERE s.row_hash <> t.row_hash
)

UPDATE target_mydataset
SET effective_to = (SELECT load_ts FROM staged LIMIT 1),
    is_current = FALSE
WHERE business_key IN (SELECT business_key FROM changed)
  AND is_current = TRUE;

INSERT INTO target_mydataset (
  business_key, attr1, attr2, effective_from, effective_to,
  is_current, row_hash
)
SELECT business_key, attr1, attr2, load_ts, NULL, TRUE, row_hash
FROM changed;

INSERT INTO target_mydataset (
  business_key, attr1, attr2, effective_from, effective_to,
  is_current, row_hash
)
SELECT s.business_key, s.attr1, s.attr2, s.load_ts, NULL, TRUE, s.row_hash
FROM staged s
LEFT JOIN target_mydataset t
       ON s.business_key = t.business_key
WHERE t.business_key IS NULL;

END;
