SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '72'
  AND id <= '324'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '72')
GROUP BY status_id;