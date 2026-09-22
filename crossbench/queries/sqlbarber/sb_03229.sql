SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '6'
  AND id <= '495'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '6')
GROUP BY status_id;