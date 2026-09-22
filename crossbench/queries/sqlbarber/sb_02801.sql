SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '192'
  AND id <= '431'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '192')
GROUP BY status_id;