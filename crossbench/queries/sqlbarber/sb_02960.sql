SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '33'
  AND id <= '383'
  AND status_id = '4'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '33')
GROUP BY status_id;