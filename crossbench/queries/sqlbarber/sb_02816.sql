SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '26'
  AND id <= '263'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '26')
GROUP BY status_id;